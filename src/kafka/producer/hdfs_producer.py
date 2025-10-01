#!/usr/bin/env python3

import json
import time
import cv2
import numpy as np
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import base64
from datetime import datetime
import logging
import threading
from typing import Dict, Any, Optional
import os
import requests
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HDFSCameraFrameProducer:
    def __init__(self, kafka_bootstrap_servers: str = "localhost:9092", 
                 namenode_host: str = "localhost", namenode_port: int = 9870,
                 datanode_port: int = 9864):
        """
        Initialize the Kafka producer for camera frames from HDFS
        
        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers connection string
            namenode_host: HDFS namenode hostname
            namenode_port: HDFS namenode WebHDFS port
            datanode_port: HDFS datanode port for direct file access
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.datanode_port = datanode_port
        self.webhdfs_url = f"http://{namenode_host}:{namenode_port}/webhdfs/v1"
        
        self.producer = None
        self.admin_client = None
        self.camera_topics = [f"camera-{i}-frames" for i in range(1, 8)]
        self.hdfs_base_path = "/surveillance/camera-feeds"
        
    def _connect_kafka(self):
        """Initialize Kafka producer and admin client"""
        try:
            # Initialize producer with custom serializers
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip'  # Compress frames for better network usage
            )
            
            # Initialize admin client for topic management
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_bootstrap_servers
            )
            
            logger.info(f"Successfully connected to Kafka at {self.kafka_bootstrap_servers}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def create_topics(self):
        """Create Kafka topics for each camera"""
        try:
            # Define topic configurations
            topics = []
            for topic_name in self.camera_topics:
                topic = NewTopic(
                    name=topic_name,
                    num_partitions=3,  # Multiple partitions for parallel processing
                    replication_factor=1  # Adjust based on your cluster setup
                )
                topics.append(topic)
            
            # Create topics
            self.admin_client.create_topics(topics, validate_only=False)
            logger.info(f"Successfully created topics: {self.camera_topics}")
            
        except TopicAlreadyExistsError:
            logger.info("Topics already exist, continuing...")
        except Exception as e:
            logger.error(f"Failed to create topics: {e}")
            raise
    
    def _get_hdfs_file_stream_url(self, hdfs_path: str) -> Optional[str]:
        """
        Get streaming URL for HDFS file with proper hostname handling
        
        Args:
            hdfs_path: HDFS file path
            
        Returns:
            Streaming URL or None if failed
        """
        try:
            url = f"{self.webhdfs_url}{hdfs_path}"
            response = requests.get(url, params={'op': 'OPEN'}, allow_redirects=False, timeout=10)
            
            if response.status_code == 307:  # Temporary Redirect
                redirect_url = response.headers.get('Location')
                if redirect_url:
                    # Fix Docker hostname issue
                    import urllib.parse
                    parsed = urllib.parse.urlparse(redirect_url)
                    # Replace any Docker container hostname with localhost
                    fixed_url = redirect_url.replace(f"{parsed.hostname}:{parsed.port}", 
                                                    f"{self.namenode_host}:{self.datanode_port}")
                    logger.info(f"Fixed streaming URL: {redirect_url} -> {fixed_url}")
                    return fixed_url
                return redirect_url
            else:
                logger.error(f"Failed to get streaming URL for {hdfs_path}: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error getting streaming URL for {hdfs_path}: {e}")
            return None
    
    def _download_hdfs_file_to_temp(self, hdfs_path: str) -> Optional[str]:
        """
        Download HDFS file to temporary location for OpenCV processing
        
        Args:
            hdfs_path: HDFS file path
            
        Returns:
            Temporary file path or None if failed
        """
        try:
            # Get streaming URL
            stream_url = self._get_hdfs_file_stream_url(hdfs_path)
            if not stream_url:
                return None
            
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
            temp_path = temp_file.name
            
            logger.info(f"Downloading {hdfs_path} from HDFS to {temp_path}")
            
            # Download file in chunks with progress
            response = requests.get(stream_url, stream=True, timeout=60)
            if response.status_code == 200:
                total_size = 0
                chunk_count = 0
                for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                    if chunk:
                        temp_file.write(chunk)
                        total_size += len(chunk)
                        chunk_count += 1
                        
                        # Log progress every 50MB
                        if chunk_count % 50 == 0:
                            logger.info(f"Downloaded {total_size / (1024*1024):.1f} MB...")
                
                temp_file.close()
                logger.info(f"Successfully downloaded {total_size / (1024*1024):.2f} MB to {temp_path}")
                return temp_path
            else:
                temp_file.close()
                os.unlink(temp_path)
                logger.error(f"Failed to download file: HTTP {response.status_code}")
                logger.error(f"Response: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading HDFS file {hdfs_path}: {e}")
            if 'temp_file' in locals():
                temp_file.close()
                if 'temp_path' in locals() and os.path.exists(temp_path):
                    os.unlink(temp_path)
            return None
    
    def _encode_frame(self, frame: np.ndarray) -> str:
        """
        Encode frame to base64 string for Kafka transmission
        
        Args:
            frame: OpenCV frame (numpy array)
            
        Returns:
            Base64 encoded string of the frame
        """
        try:
            # Encode frame to JPEG format
            _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
            # Convert to base64 string
            frame_base64 = base64.b64encode(buffer).decode('utf-8')
            return frame_base64
        except Exception as e:
            logger.error(f"Failed to encode frame: {e}")
            return None
    
    def _create_message(self, cam_id: int, frame_id: int, frame: np.ndarray) -> Dict[str, Any]:
        """
        Create message payload with metadata
        
        Args:
            cam_id: Camera ID (1-7)
            frame_id: Sequential frame number
            frame: OpenCV frame
            
        Returns:
            Dictionary containing frame data and metadata
        """
        timestamp = datetime.now().isoformat()
        frame_encoded = self._encode_frame(frame)
        
        if frame_encoded is None:
            return None
        
        message = {
            "metadata": {
                "camId": cam_id,
                "frameId": frame_id,
                "timestamp": timestamp,
                "frame_shape": frame.shape,
                "encoding": "base64_jpeg",
                "source": "hdfs"
            },
            "frame_data": frame_encoded
        }
        
        return message
    
    def process_camera_feed_from_hdfs(self, cam_id: int, max_frames: int = None):
        """
        Process a single camera feed from HDFS and publish frames to Kafka
        
        Args:
            cam_id: Camera ID (1-7)
            max_frames: Maximum number of frames to process (None for all)
        """
        hdfs_file_path = f"{self.hdfs_base_path}/cam{cam_id}.mp4"
        topic_name = self.camera_topics[cam_id - 1]
        
        logger.info(f"Starting to process camera {cam_id} from HDFS: {hdfs_file_path}")
        
        # Download file from HDFS to temporary location
        temp_file_path = self._download_hdfs_file_to_temp(hdfs_file_path)
        if not temp_file_path:
            logger.error(f"Failed to download camera {cam_id} from HDFS")
            return
        
        try:
            # Open video capture from temporary file
            cap = cv2.VideoCapture(temp_file_path)
            
            if not cap.isOpened():
                logger.error(f"Failed to open video file: {temp_file_path}")
                return
            
            frame_id = 0
            frames_published = 0
            
            # Get video properties
            fps = cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            
            logger.info(f"Camera {cam_id}: FPS={fps}, Total frames={total_frames}")
            
            while True:
                ret, frame = cap.read()
                
                if not ret:
                    logger.info(f"Camera {cam_id}: End of video reached")
                    break
                
                if max_frames and frames_published >= max_frames:
                    logger.info(f"Camera {cam_id}: Reached max frames limit ({max_frames})")
                    break
                
                # Create message
                message = self._create_message(cam_id, frame_id, frame)
                
                if message is not None:
                    try:
                        # Send message to Kafka
                        future = self.producer.send(
                            topic_name,
                            key=f"cam{cam_id}_frame{frame_id}",
                            value=message
                        )
                        
                        frames_published += 1
                        
                        if frames_published % 100 == 0:
                            logger.info(f"Camera {cam_id}: Published {frames_published} frames")
                        
                    except Exception as e:
                        logger.error(f"Failed to send frame {frame_id} from camera {cam_id}: {e}")
                
                frame_id += 1
                
                # Optional: Add delay to simulate real-time streaming
                # time.sleep(1/fps)
            
            logger.info(f"Camera {cam_id}: Finished processing. Total frames published: {frames_published}")
            
        finally:
            if 'cap' in locals():
                cap.release()
            
            # Clean up temporary file
            try:
                os.unlink(temp_file_path)
                logger.info(f"Cleaned up temporary file: {temp_file_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary file {temp_file_path}: {e}")
    
    def start_all_cameras(self, max_frames_per_camera: int = None, use_threading: bool = True):
        """
        Start processing all camera feeds from HDFS
        
        Args:
            max_frames_per_camera: Maximum frames to process per camera
            use_threading: Whether to use threading for parallel processing
        """
        if not self._connect_kafka():
            logger.error("Failed to connect to Kafka. Exiting.")
            return
        
        # Create topics
        self.create_topics()
        
        logger.info("Starting to process all camera feeds from HDFS...")
        
        if use_threading:
            # Process cameras in parallel using threads
            threads = []
            
            for cam_id in range(1, 8):
                thread = threading.Thread(
                    target=self.process_camera_feed_from_hdfs,
                    args=(cam_id, max_frames_per_camera),
                    name=f"Camera-{cam_id}-Thread"
                )
                threads.append(thread)
                thread.start()
            
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
                
        else:
            # Process cameras sequentially
            for cam_id in range(1, 8):
                self.process_camera_feed_from_hdfs(cam_id, max_frames_per_camera)
        
        # Flush and close producer
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        logger.info("All camera feeds processed successfully!")
    
    def start_single_camera(self, cam_id: int, max_frames: int = None):
        """
        Start processing a single camera feed from HDFS
        
        Args:
            cam_id: Camera ID (1-7)
            max_frames: Maximum number of frames to process
        """
        if not (1 <= cam_id <= 7):
            logger.error("Camera ID must be between 1 and 7")
            return
        
        if not self._connect_kafka():
            logger.error("Failed to connect to Kafka. Exiting.")
            return
        
        # Create topics
        self.create_topics()
        
        # Process single camera
        self.process_camera_feed_from_hdfs(cam_id, max_frames)
        
        # Flush and close producer
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        logger.info(f"Camera {cam_id} processing completed!")


def main():
    """Main function to run the HDFS producer"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Camera Frame Producer from HDFS")
    parser.add_argument("--kafka-servers", default="localhost:9092", 
                       help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--namenode-host", default="localhost", 
                       help="HDFS namenode hostname (default: localhost)")
    parser.add_argument("--namenode-port", type=int, default=9870, 
                       help="HDFS namenode WebHDFS port (default: 9870)")
    parser.add_argument("--datanode-port", type=int, default=9864, 
                       help="HDFS datanode port (default: 9864)")
    parser.add_argument("--camera", type=int, choices=range(1, 8), 
                       help="Process single camera (1-7)")
    parser.add_argument("--max-frames", type=int, 
                       help="Maximum frames to process per camera")
    parser.add_argument("--no-threading", action="store_true", 
                       help="Disable threading for sequential processing")
    
    args = parser.parse_args()
    
    # Initialize producer
    producer = HDFSCameraFrameProducer(
        kafka_bootstrap_servers=args.kafka_servers,
        namenode_host=args.namenode_host,
        namenode_port=args.namenode_port,
        datanode_port=args.datanode_port
    )
    
    try:
        if args.camera:
            # Process single camera
            producer.start_single_camera(args.camera, args.max_frames)
        else:
            # Process all cameras
            use_threading = not args.no_threading
            producer.start_all_cameras(args.max_frames, use_threading)
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Stopping producer...")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()