#!/usr/bin/env python3

import json
import base64
import cv2
import numpy as np
from flask import Flask, render_template, jsonify, Response
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
import threading
import time
from datetime import datetime
import logging
from typing import Dict, Any, List
import queue
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global state
app = Flask(__name__)
app.config['SECRET_KEY'] = 'surveillance-webui-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

class SurveillanceWebUI:
    def __init__(self, kafka_bootstrap_servers="localhost:9092"):
        self.kafka_servers = kafka_bootstrap_servers
        self.frame_consumers = {}
        self.detection_consumers = {}
        self.frame_queues = {}
        self.detection_queues = {}
        self.statistics = {
            'cameras': {i: {
                'total_frames': 0,
                'total_detections': 0,
                'detections_by_class': {},
                'last_frame_time': None,
                'last_detection_time': None,
                'fps': 0,
                'detection_rate': 0
            } for i in range(1, 8)},
            'global': {
                'total_frames': 0,
                'total_detections': 0,
                'active_cameras': set(),
                'start_time': time.time()
            }
        }
        
        # Frame and detection storage
        self.latest_frames = {}
        self.latest_detections = {}
        
        # Initialize queues
        for i in range(1, 8):
            self.frame_queues[i] = queue.Queue(maxsize=10)
            self.detection_queues[i] = queue.Queue(maxsize=50)
        
        self.running = False
        self.threads = []
    
    def start_kafka_consumers(self):
        """Start Kafka consumers for all cameras"""
        self.running = True
        
        # Start frame consumers
        for cam_id in range(1, 8):
            frame_thread = threading.Thread(
                target=self._consume_frames,
                args=(cam_id,),
                daemon=True
            )
            frame_thread.start()
            self.threads.append(frame_thread)
            
            detection_thread = threading.Thread(
                target=self._consume_detections,
                args=(cam_id,),
                daemon=True
            )
            detection_thread.start()
            self.threads.append(detection_thread)
        
        # Start statistics updater
        stats_thread = threading.Thread(
            target=self._update_statistics,
            daemon=True
        )
        stats_thread.start()
        self.threads.append(stats_thread)
        
        logger.info("Started all Kafka consumers and statistics updater")
    
    def _consume_frames(self, cam_id: int):
        """Consume frames from camera topic"""
        topic = f"camera-{cam_id}-frames"
        
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.kafka_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=f'webui-frames-cam{cam_id}',
                auto_offset_reset='latest',
                consumer_timeout_ms=1000
            )
            
            logger.info(f"Started frame consumer for camera {cam_id}")
            
            while self.running:
                try:
                    for message in consumer:
                        if not self.running:
                            break
                        
                        frame_data = message.value
                        metadata = frame_data.get('metadata', {})
                        frame_base64 = frame_data.get('frame_data')
                        
                        if frame_base64:
                            # Store latest frame
                            self.latest_frames[cam_id] = {
                                'frame_data': frame_base64,
                                'metadata': metadata,
                                'timestamp': time.time()
                            }
                            
                            # Update statistics
                            self.statistics['cameras'][cam_id]['total_frames'] += 1
                            self.statistics['cameras'][cam_id]['last_frame_time'] = time.time()
                            self.statistics['global']['total_frames'] += 1
                            self.statistics['global']['active_cameras'].add(cam_id)
                            
                            # Add to queue (non-blocking)
                            try:
                                self.frame_queues[cam_id].put_nowait(frame_data)
                            except queue.Full:
                                # Remove oldest frame if queue is full
                                try:
                                    self.frame_queues[cam_id].get_nowait()
                                    self.frame_queues[cam_id].put_nowait(frame_data)
                                except queue.Empty:
                                    pass
                            
                            # Emit to WebSocket clients
                            socketio.emit('frame_update', {
                                'camera_id': cam_id,
                                'frame_id': metadata.get('frameId'),
                                'timestamp': metadata.get('timestamp')
                            })
                        
                except Exception as e:
                    logger.error(f"Error in frame consumer for camera {cam_id}: {e}")
                    time.sleep(1)
            
        except Exception as e:
            logger.error(f"Failed to start frame consumer for camera {cam_id}: {e}")
    
    def _consume_detections(self, cam_id: int):
        """Consume detections from camera detection topic"""
        topic = f"camera-{cam_id}-detections"
        
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.kafka_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=f'webui-detections-cam{cam_id}',
                auto_offset_reset='latest',
                consumer_timeout_ms=1000
            )
            
            logger.info(f"Started detection consumer for camera {cam_id}")
            
            while self.running:
                try:
                    for message in consumer:
                        if not self.running:
                            break
                        
                        detection_data = message.value
                        metadata = detection_data.get('metadata', {})
                        detections = detection_data.get('detections', [])
                        
                        # Store latest detections
                        self.latest_detections[cam_id] = {
                            'detections': detections,
                            'metadata': metadata,
                            'timestamp': time.time()
                        }
                        
                        # Update statistics
                        self.statistics['cameras'][cam_id]['total_detections'] += len(detections)
                        self.statistics['cameras'][cam_id]['last_detection_time'] = time.time()
                        self.statistics['global']['total_detections'] += len(detections)
                        
                        # Update class counts
                        for detection in detections:
                            class_name = detection.get('class_name', 'unknown')
                            if class_name not in self.statistics['cameras'][cam_id]['detections_by_class']:
                                self.statistics['cameras'][cam_id]['detections_by_class'][class_name] = 0
                            self.statistics['cameras'][cam_id]['detections_by_class'][class_name] += 1
                        
                        # Add to queue (non-blocking)
                        try:
                            self.detection_queues[cam_id].put_nowait(detection_data)
                        except queue.Full:
                            try:
                                self.detection_queues[cam_id].get_nowait()
                                self.detection_queues[cam_id].put_nowait(detection_data)
                            except queue.Empty:
                                pass
                        
                        # Emit to WebSocket clients
                        socketio.emit('detection_update', {
                            'camera_id': cam_id,
                            'detection_count': len(detections),
                            'classes': list(set(d.get('class_name') for d in detections))
                        })
                        
                except Exception as e:
                    logger.error(f"Error in detection consumer for camera {cam_id}: {e}")
                    time.sleep(1)
                    
        except Exception as e:
            logger.error(f"Failed to start detection consumer for camera {cam_id}: {e}")
    
    def _update_statistics(self):
        """Update FPS and detection rates"""
        frame_counts = {i: 0 for i in range(1, 8)}
        detection_counts = {i: 0 for i in range(1, 8)}
        
        while self.running:
            time.sleep(5)  # Update every 5 seconds
            
            current_time = time.time()
            
            for cam_id in range(1, 8):
                # Calculate FPS
                prev_frames = frame_counts[cam_id]
                current_frames = self.statistics['cameras'][cam_id]['total_frames']
                frame_diff = current_frames - prev_frames
                self.statistics['cameras'][cam_id]['fps'] = frame_diff / 5.0
                frame_counts[cam_id] = current_frames
                
                # Calculate detection rate
                prev_detections = detection_counts[cam_id]
                current_detections = self.statistics['cameras'][cam_id]['total_detections']
                detection_diff = current_detections - prev_detections
                self.statistics['cameras'][cam_id]['detection_rate'] = detection_diff / 5.0
                detection_counts[cam_id] = current_detections
            
            # Emit statistics update
            socketio.emit('statistics_update', self.get_statistics())
    
    def get_statistics(self):
        """Get current statistics"""
        current_time = time.time()
        uptime = current_time - self.statistics['global']['start_time']
        
        stats = {
            'global': {
                **self.statistics['global'],
                'active_cameras': list(self.statistics['global']['active_cameras']),
                'uptime': uptime
            },
            'cameras': {}
        }
        
        for cam_id in range(1, 8):
            cam_stats = self.statistics['cameras'][cam_id].copy()
            
            # Add status indicators
            last_frame_time = cam_stats.get('last_frame_time')
            last_detection_time = cam_stats.get('last_detection_time')
            
            cam_stats['status'] = 'active' if (
                last_frame_time and current_time - last_frame_time < 10
            ) else 'inactive'
            
            cam_stats['detection_status'] = 'active' if (
                last_detection_time and current_time - last_detection_time < 30
            ) else 'inactive'
            
            stats['cameras'][cam_id] = cam_stats
        
        return stats
    
    def get_frame_image(self, cam_id: int, show_detections: bool = False):
        """Get frame image for streaming"""
        if cam_id not in self.latest_frames:
            return None
        
        try:
            frame_info = self.latest_frames[cam_id]
            frame_base64 = frame_info['frame_data']
            
            # Decode frame
            frame_bytes = base64.b64decode(frame_base64)
            nparr = np.frombuffer(frame_bytes, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if frame is None:
                return None
            
            # Draw detections if requested and available
            if show_detections and cam_id in self.latest_detections:
                detection_info = self.latest_detections[cam_id]
                detections = detection_info['detections']
                
                for detection in detections:
                    bbox = detection.get('bbox', {})
                    x = bbox.get('x', 0)
                    y = bbox.get('y', 0)
                    w = bbox.get('width', 0)
                    h = bbox.get('height', 0)
                    
                    class_name = detection.get('class_name', 'unknown')
                    confidence = detection.get('confidence', 0)
                    
                    # Draw bounding box
                    cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 255, 0), 2)
                    
                    # Draw label
                    label = f"{class_name}: {confidence:.2f}"
                    label_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.6, 2)[0]
                    cv2.rectangle(frame, (x, y - label_size[1] - 10), 
                                (x + label_size[0], y), (0, 255, 0), -1)
                    cv2.putText(frame, label, (x, y - 5), 
                              cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 0), 2)
            
            # Encode frame as JPEG
            _, jpeg = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
            return jpeg.tobytes()
            
        except Exception as e:
            logger.error(f"Error processing frame for camera {cam_id}: {e}")
            return None
    
    def stop(self):
        """Stop all consumers"""
        self.running = False
        logger.info("Stopping surveillance web UI...")

# Global instance
surveillance_ui = SurveillanceWebUI()

# Flask routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/statistics')
def get_statistics():
    return jsonify(surveillance_ui.get_statistics())

@app.route('/api/cameras')
def get_cameras():
    return jsonify({
        'cameras': list(range(1, 8)),
        'active_cameras': list(surveillance_ui.statistics['global']['active_cameras'])
    })

@app.route('/video_feed/<int:cam_id>')
def video_feed(cam_id):
    def generate():
        while surveillance_ui.running:
            frame = surveillance_ui.get_frame_image(cam_id, show_detections=False)
            if frame:
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n\r\n')
            time.sleep(0.1)  # Limit to ~10 FPS for web display
    
    return Response(generate(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/detection_feed/<int:cam_id>')
def detection_feed(cam_id):
    def generate():
        while surveillance_ui.running:
            frame = surveillance_ui.get_frame_image(cam_id, show_detections=True)
            if frame:
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n\r\n')
            time.sleep(0.1)  # Limit to ~10 FPS for web display
    
    return Response(generate(), mimetype='multipart/x-mixed-replace; boundary=frame')

# WebSocket events
@socketio.on('connect')
def handle_connect():
    emit('connected', {'status': 'Connected to Surveillance WebUI'})
    emit('statistics_update', surveillance_ui.get_statistics())

@socketio.on('get_statistics')
def handle_get_statistics():
    emit('statistics_update', surveillance_ui.get_statistics())

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Surveillance Web UI")
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--host", default="0.0.0.0",
                       help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5003,
                       help="Port to bind to (default: 5000)")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug mode")
    
    args = parser.parse_args()
    
    # Initialize surveillance UI
    global surveillance_ui
    surveillance_ui = SurveillanceWebUI(args.kafka_servers)
    
    # Start Kafka consumers
    surveillance_ui.start_kafka_consumers()
    
    try:
        print(f"🌐 Starting Surveillance Web UI...")
        print(f"📡 Connecting to Kafka: {args.kafka_servers}")
        print(f"🖥️  Web interface: http://{args.host}:{args.port}")
        print("🎥 Camera feeds will appear as data flows in...")
        
        # Start Flask app
        socketio.run(app, host=args.host, port=args.port, debug=args.debug)
        
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    finally:
        surveillance_ui.stop()

if __name__ == "__main__":
    main()