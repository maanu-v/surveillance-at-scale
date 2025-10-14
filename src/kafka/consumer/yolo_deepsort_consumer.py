#!/usr/bin/env python3

import json
import base64
import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import logging
from typing import Dict, Any, List, Tuple
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YOLODetection:
    """Class to represent a YOLO detection"""
    def __init__(self, class_id: int, class_name: str, confidence: float, bbox: Tuple[int, int, int, int]):
        self.class_id = class_id
        self.class_name = class_name
        self.confidence = confidence
        self.bbox = bbox  # (x, y, width, height)
        
    def __str__(self):
        return f"Detection(class={self.class_name}, conf={self.confidence:.3f}, bbox={self.bbox})"

class YOLODeepSortConsumer:
    """YOLO consumer for object detection and tracking on Kafka streams"""
    
    # COCO class names for YOLO
    COCO_CLASSES = [
        'person', 'bicycle', 'car', 'motorcycle', 'airplane', 'bus', 'train', 'truck',
        'boat', 'traffic light', 'fire hydrant', 'stop sign', 'parking meter', 'bench',
        'bird', 'cat', 'dog', 'horse', 'sheep', 'cow', 'elephant', 'bear', 'zebra',
        'giraffe', 'backpack', 'umbrella', 'handbag', 'tie', 'suitcase', 'frisbee',
        'skis', 'snowboard', 'sports ball', 'kite', 'baseball bat', 'baseball glove',
        'skateboard', 'surfboard', 'tennis racket', 'bottle', 'wine glass', 'cup',
        'fork', 'knife', 'spoon', 'bowl', 'banana', 'apple', 'sandwich', 'orange',
        'broccoli', 'carrot', 'hot dog', 'pizza', 'donut', 'cake', 'chair', 'couch',
        'potted plant', 'bed', 'dining table', 'toilet', 'tv', 'laptop', 'mouse',
        'remote', 'keyboard', 'cell phone', 'microwave', 'oven', 'toaster', 'sink',
        'refrigerator', 'book', 'clock', 'vase', 'scissors', 'teddy bear', 'hair drier',
        'toothbrush'
    ]

    def __init__(self, kafka_bootstrap_servers: str = "localhost:9092", 
                 confidence_threshold: float = 0.5,
                 nms_threshold: float = 0.4,
                 publish_detections: bool = True):
        """
        Initialize the YOLO DeepSORT Kafka consumer
        
        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers connection string
            confidence_threshold: Minimum confidence for detections
            nms_threshold: Non-maximum suppression threshold
            publish_detections: Whether to publish detection results to Kafka
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.confidence_threshold = confidence_threshold
        self.nms_threshold = nms_threshold
        self.publish_detections = publish_detections
        self.consumer = None
        self.producer = None
        self.camera_topics = [f"camera-{i}-frames" for i in range(1, 8)]
        self.detection_topics = [f"camera-{i}-detections" for i in range(1, 8)]
        
        # Detection statistics
        self.detection_stats = {
            'total_frames': 0,
            'total_detections': 0,
            'detections_by_class': {},
            'detections_by_camera': {},
            'total_published': 0,
            'start_time': None
        }
        
        # Initialize YOLO (using OpenCV's DNN module for simplicity)
        # Note: You would need to download YOLO weights, config, and names files
        self.net = None
        self.output_layers = None
        self._initialize_yolo()
        
        # Initialize Kafka producer for publishing detection results
        if self.publish_detections:
            self._initialize_producer()
    
    def _initialize_yolo(self):
        """Initialize YOLO model using OpenCV DNN"""
        try:
            # Note: These paths would need to be updated with actual YOLO model files
            # For demonstration purposes, we'll simulate YOLO detection
            logger.info("YOLO model initialization simulated (replace with actual model loading)")
            logger.info("To use real YOLO, download yolov4.weights, yolov4.cfg, and coco.names")
            
            # Uncomment and modify these lines to load actual YOLO model:
            # self.net = cv2.dnn.readNet("yolov4.weights", "yolov4.cfg")
            # layer_names = self.net.getLayerNames()
            # self.output_layers = [layer_names[i[0] - 1] for i in self.net.getUnconnectedOutLayers()]
            
        except Exception as e:
            logger.error(f"Failed to initialize YOLO: {e}")
            logger.info("Continuing with simulated detections...")
    
    def _initialize_producer(self):
        """Initialize Kafka producer for publishing detection results"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip'
            )
            logger.info("Successfully initialized Kafka producer for detection results")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            self.publish_detections = False
            return False
    
    def _connect_kafka(self, topics: list):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8'),
                group_id='yolo-detection-consumers',
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                consumer_timeout_ms=1000  # Timeout for polling
            )
            
            logger.info(f"Successfully connected to Kafka at {self.kafka_bootstrap_servers}")
            logger.info(f"Subscribed to topics: {topics}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def _decode_frame(self, frame_data: str) -> np.ndarray:
        """
        Decode base64 frame data back to OpenCV frame
        
        Args:
            frame_data: Base64 encoded frame string
            
        Returns:
            OpenCV frame (numpy array)
        """
        try:
            frame_bytes = base64.b64decode(frame_data)
            nparr = np.frombuffer(frame_bytes, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            return frame
        except Exception as e:
            logger.error(f"Failed to decode frame: {e}")
            return None
    
    def _simulate_yolo_detection(self, frame: np.ndarray) -> List[YOLODetection]:
        """
        Simulate YOLO detections for demonstration
        Replace this with actual YOLO inference
        
        Args:
            frame: Input frame
            
        Returns:
            List of detections
        """
        detections = []
        
        # Simulate some random detections for demonstration
        height, width = frame.shape[:2]
        
        # Simulate person detections (most common in surveillance)
        if np.random.random() > 0.3:  # 70% chance of detecting a person
            x = np.random.randint(0, width // 2)
            y = np.random.randint(0, height // 2)
            w = np.random.randint(50, width // 4)
            h = np.random.randint(100, height // 3)
            conf = np.random.uniform(0.6, 0.95)
            
            detection = YOLODetection(0, "person", conf, (x, y, w, h))
            detections.append(detection)
        
        # Simulate car detections
        if np.random.random() > 0.7:  # 30% chance of detecting a car
            x = np.random.randint(0, width // 2)
            y = np.random.randint(height // 2, height - 100)
            w = np.random.randint(80, width // 3)
            h = np.random.randint(40, height // 4)
            conf = np.random.uniform(0.5, 0.9)
            
            detection = YOLODetection(2, "car", conf, (x, y, w, h))
            detections.append(detection)
        
        return detections
    
    def _run_yolo_detection(self, frame: np.ndarray) -> List[YOLODetection]:
        """
        Run YOLO detection on frame
        
        Args:
            frame: Input frame
            
        Returns:
            List of YOLODetection objects
        """
        if self.net is None:
            # Use simulated detection if no model is loaded
            return self._simulate_yolo_detection(frame)
        
        # Real YOLO detection code would go here
        try:
            height, width = frame.shape[:2]
            
            # Create blob from image
            blob = cv2.dnn.blobFromImage(frame, 1/255.0, (416, 416), swapRB=True, crop=False)
            self.net.setInput(blob)
            
            # Run forward pass
            outputs = self.net.forward(self.output_layers)
            
            # Parse detections
            detections = []
            boxes = []
            confidences = []
            class_ids = []
            
            for output in outputs:
                for detection in output:
                    scores = detection[5:]
                    class_id = np.argmax(scores)
                    confidence = scores[class_id]
                    
                    if confidence > self.confidence_threshold:
                        # Scale bounding box back to original image size
                        center_x = int(detection[0] * width)
                        center_y = int(detection[1] * height)
                        w = int(detection[2] * width)
                        h = int(detection[3] * height)
                        x = int(center_x - w / 2)
                        y = int(center_y - h / 2)
                        
                        boxes.append([x, y, w, h])
                        confidences.append(float(confidence))
                        class_ids.append(class_id)
            
            # Apply non-maximum suppression
            indices = cv2.dnn.NMSBoxes(boxes, confidences, self.confidence_threshold, self.nms_threshold)
            
            if len(indices) > 0:
                for i in indices.flatten():
                    class_name = self.COCO_CLASSES[class_ids[i]] if class_ids[i] < len(self.COCO_CLASSES) else "unknown"
                    detection = YOLODetection(class_ids[i], class_name, confidences[i], tuple(boxes[i]))
                    detections.append(detection)
            
            return detections
            
        except Exception as e:
            logger.error(f"YOLO detection failed: {e}")
            return []
    
    def _create_detection_message(self, camera_id: int, frame_id: int, timestamp: str, 
                                detections: List[YOLODetection], frame_metadata: Dict) -> Dict[str, Any]:
        """
        Create detection message for Kafka publication
        
        Args:
            camera_id: Camera ID
            frame_id: Frame ID
            timestamp: Frame timestamp
            detections: List of detections
            frame_metadata: Original frame metadata
            
        Returns:
            Detection message dictionary
        """
        detection_data = []
        
        for detection in detections:
            x, y, w, h = detection.bbox
            detection_info = {
                "class_id": detection.class_id,
                "class_name": detection.class_name,
                "confidence": round(detection.confidence, 4),
                "bbox": {
                    "x": x,
                    "y": y,
                    "width": w,
                    "height": h,
                    "center_x": x + w // 2,
                    "center_y": y + h // 2
                }
            }
            detection_data.append(detection_info)
        
        # Create detection summary
        class_counts = {}
        for detection in detections:
            class_counts[detection.class_name] = class_counts.get(detection.class_name, 0) + 1
        
        message = {
            "metadata": {
                "camera_id": camera_id,
                "frame_id": frame_id,
                "timestamp": timestamp,
                "detection_timestamp": datetime.now().isoformat(),
                "total_detections": len(detections),
                "detection_classes": list(class_counts.keys()),
                "class_counts": class_counts,
                "confidence_threshold": self.confidence_threshold,
                "nms_threshold": self.nms_threshold,
                "original_frame_metadata": frame_metadata
            },
            "detections": detection_data
        }
        
        return message
    
    def _publish_detection_results(self, camera_id: int, frame_id: int, timestamp: str,
                                 detections: List[YOLODetection], frame_metadata: Dict):
        """
        Publish detection results to Kafka topic
        
        Args:
            camera_id: Camera ID
            frame_id: Frame ID
            timestamp: Frame timestamp
            detections: List of detections
            frame_metadata: Original frame metadata
        """
        if not self.publish_detections or not self.producer:
            return
        
        try:
            # Create detection message
            detection_message = self._create_detection_message(
                camera_id, frame_id, timestamp, detections, frame_metadata
            )
            
            # Determine topic name
            detection_topic = f"camera-{camera_id}-detections"
            
            # Create message key
            message_key = f"cam{camera_id}_frame{frame_id}_detections"
            
            # Send to Kafka
            future = self.producer.send(
                detection_topic,
                key=message_key,
                value=detection_message
            )
            
            # Optional: Wait for confirmation (uncomment for guaranteed delivery)
            # future.get(timeout=5)
            
            # Update published statistics
            self.detection_stats['total_published'] += 1
            
            logger.debug(f"Published {len(detections)} detections for camera {camera_id} frame {frame_id}")
            
        except Exception as e:
            logger.error(f"Failed to publish detection results: {e}")
    
    def _update_statistics(self, camera_id: int, detections: List[YOLODetection]):
        """Update detection statistics"""
        self.detection_stats['total_frames'] += 1
        self.detection_stats['total_detections'] += len(detections)
        
        # Update per-camera stats
        if camera_id not in self.detection_stats['detections_by_camera']:
            self.detection_stats['detections_by_camera'][camera_id] = 0
        self.detection_stats['detections_by_camera'][camera_id] += len(detections)
        
        # Update per-class stats
        for detection in detections:
            if detection.class_name not in self.detection_stats['detections_by_class']:
                self.detection_stats['detections_by_class'][detection.class_name] = 0
            self.detection_stats['detections_by_class'][detection.class_name] += 1
    
    def _print_detections(self, camera_id: int, frame_id: int, timestamp: str, detections: List[YOLODetection]):
        """Print detection results in a formatted way"""
        if detections:
            print(f"\n{'='*80}")
            print(f"🎯 DETECTIONS - Camera {camera_id} | Frame {frame_id} | {timestamp}")
            print(f"{'='*80}")
            
            for i, detection in enumerate(detections, 1):
                print(f"  {i:2d}. {detection}")
            
            print(f"📊 Total detections: {len(detections)}")
            
            # Group by class
            class_counts = {}
            for detection in detections:
                class_counts[detection.class_name] = class_counts.get(detection.class_name, 0) + 1
            
            print(f"📋 By class: {dict(class_counts)}")
        else:
            print(f"⭕ No detections - Camera {camera_id} | Frame {frame_id}")
    
    def _print_statistics(self):
        """Print overall detection statistics"""
        if self.detection_stats['start_time']:
            elapsed = time.time() - self.detection_stats['start_time']
            fps = self.detection_stats['total_frames'] / elapsed if elapsed > 0 else 0
            
            print(f"\n{'='*80}")
            print(f"📈 DETECTION STATISTICS")
            print(f"{'='*80}")
            print(f"🕒 Runtime: {elapsed:.2f} seconds")
            print(f"🎬 Total frames processed: {self.detection_stats['total_frames']}")
            print(f"🎯 Total detections: {self.detection_stats['total_detections']}")
            print(f"📤 Detection messages published: {self.detection_stats['total_published']}")
            print(f"⚡ Processing rate: {fps:.2f} FPS")
            
            if self.detection_stats['total_frames'] > 0:
                avg_detections = self.detection_stats['total_detections'] / self.detection_stats['total_frames']
                print(f"📊 Average detections per frame: {avg_detections:.2f}")
            
            if self.detection_stats['detections_by_class']:
                print(f"\n🏷️  Detections by class:")
                for class_name, count in sorted(self.detection_stats['detections_by_class'].items()):
                    print(f"   {class_name}: {count}")
            
            if self.detection_stats['detections_by_camera']:
                print(f"\n📹 Detections by camera:")
                for camera_id, count in sorted(self.detection_stats['detections_by_camera'].items()):
                    print(f"   Camera {camera_id}: {count}")
            print(f"{'='*80}\n")
    
    def consume_with_yolo_detection(self, topics: List[str] = None, max_messages: int = None, 
                                  print_interval: int = 10, save_frames: bool = False):
        """
        Consume messages and run YOLO detection
        
        Args:
            topics: List of topics to subscribe to (default: all camera topics)
            max_messages: Maximum number of messages to process
            print_interval: Print statistics every N frames
            save_frames: Whether to save frames with detections
        """
        if topics is None:
            topics = self.camera_topics
        
        if not self._connect_kafka(topics):
            logger.error("Failed to connect to Kafka. Exiting.")
            return
        
        logger.info(f"Starting YOLO detection consumer...")
        logger.info(f"Confidence threshold: {self.confidence_threshold}")
        logger.info(f"NMS threshold: {self.nms_threshold}")
        
        self.detection_stats['start_time'] = time.time()
        
        try:
            message_count = 0
            
            for message in self.consumer:
                try:
                    # Extract message data
                    frame_data = message.value
                    metadata = frame_data.get('metadata', {})
                    frame_base64 = frame_data.get('frame_data')
                    
                    if not frame_base64:
                        logger.warning("Received message without frame data")
                        continue
                    
                    # Decode frame
                    frame = self._decode_frame(frame_base64)
                    if frame is None:
                        logger.warning("Failed to decode frame")
                        continue
                    
                    # Run YOLO detection
                    detections = self._run_yolo_detection(frame)
                    
                    # Update statistics
                    camera_id = metadata.get('camId', 'unknown')
                    frame_id = metadata.get('frameId', message_count)
                    timestamp = metadata.get('timestamp', datetime.now().isoformat())
                    
                    self._update_statistics(camera_id, detections)
                    
                    # Publish detection results to Kafka
                    self._publish_detection_results(camera_id, frame_id, timestamp, detections, metadata)
                    
                    # Print detection results
                    self._print_detections(camera_id, frame_id, timestamp, detections)
                    
                    # Save frame with detections if requested
                    if save_frames and detections:
                        self._save_detection_frame(frame, detections, camera_id, frame_id)
                    
                    message_count += 1
                    
                    # Print statistics periodically
                    if message_count % print_interval == 0:
                        self._print_statistics()
                    
                    if max_messages and message_count >= max_messages:
                        logger.info(f"Reached max messages limit ({max_messages})")
                        break
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
            
        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Stopping consumer...")
        finally:
            if self.consumer:
                self.consumer.close()
            
            if self.producer:
                logger.info("Flushing and closing Kafka producer...")
                self.producer.flush()
                self.producer.close()
            
            # Print final statistics
            self._print_statistics()
    
    def _save_detection_frame(self, frame: np.ndarray, detections: List[YOLODetection], 
                            camera_id: int, frame_id: int):
        """Save frame with detection bounding boxes drawn"""
        try:
            # Draw bounding boxes
            annotated_frame = frame.copy()
            
            for detection in detections:
                x, y, w, h = detection.bbox
                
                # Draw bounding box
                cv2.rectangle(annotated_frame, (x, y), (x + w, y + h), (0, 255, 0), 2)
                
                # Draw label
                label = f"{detection.class_name}: {detection.confidence:.2f}"
                label_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 2)[0]
                cv2.rectangle(annotated_frame, (x, y - label_size[1] - 10), 
                            (x + label_size[0], y), (0, 255, 0), -1)
                cv2.putText(annotated_frame, label, (x, y - 5), 
                          cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0), 2)
            
            # Save annotated frame
            filename = f"detection_cam{camera_id}_frame{frame_id}.jpg"
            cv2.imwrite(filename, annotated_frame)
            logger.info(f"Saved detection frame: {filename}")
            
        except Exception as e:
            logger.error(f"Failed to save detection frame: {e}")

    def consume_single_camera(self, cam_id: int, max_messages: int = None):
        """
        Consume from a single camera topic with YOLO detection
        
        Args:
            cam_id: Camera ID (1-7)
            max_messages: Maximum number of messages to consume
        """
        if not (1 <= cam_id <= 7):
            logger.error("Camera ID must be between 1 and 7")
            return
        
        topic = f"camera-{cam_id}-frames"
        self.consume_with_yolo_detection([topic], max_messages)


def main():
    """Main function to run the YOLO consumer"""
    import argparse
    
    parser = argparse.ArgumentParser(description="YOLO DeepSORT Kafka Consumer")
    parser.add_argument("--kafka-servers", default="localhost:9092", 
                       help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--camera", type=int, choices=range(1, 8), 
                       help="Consume from single camera (1-7)")
    parser.add_argument("--max-messages", type=int, 
                       help="Maximum messages to consume")
    parser.add_argument("--confidence", type=float, default=0.5,
                       help="Confidence threshold for detections (default: 0.5)")
    parser.add_argument("--nms", type=float, default=0.4,
                       help="NMS threshold (default: 0.4)")
    parser.add_argument("--print-interval", type=int, default=10,
                       help="Print statistics every N frames (default: 10)")
    parser.add_argument("--save-frames", action="store_true",
                       help="Save frames with detections")
    parser.add_argument("--no-publish", action="store_true",
                       help="Disable publishing detection results to Kafka")
    
    args = parser.parse_args()
    
    # Initialize consumer
    consumer = YOLODeepSortConsumer(
        kafka_bootstrap_servers=args.kafka_servers,
        confidence_threshold=args.confidence,
        nms_threshold=args.nms,
        publish_detections=not args.no_publish
    )
    
    try:
        if args.camera:
            # Consume from single camera
            consumer.consume_single_camera(args.camera, args.max_messages)
        else:
            # Consume from all cameras
            consumer.consume_with_yolo_detection(
                max_messages=args.max_messages,
                print_interval=args.print_interval,
                save_frames=args.save_frames
            )
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Stopping consumer...")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()
