
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
from typing import Dict, Any
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CameraFrameProducer:
    def __init__(self, kafka_bootstrap_servers: str = "localhost:9092"):
        """
        Initialize the Kafka producer for camera frames
        
        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers connection string
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = None
        self.admin_client = None
        self.camera_topics = [f"camera-{i}-frames" for i in range(1, 8)]
        self.data_path = "data/raw/test/"
        self.camera_files = [f"{self.data_path}cam{i}.mp4" for i in range(1, 8)]
        
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
                "encoding": "base64_jpeg"
            },
            "frame_data": frame_encoded
        }
        
        return message
    
    def process_camera_feed(self, cam_id: int, max_frames: int = None):
        """
        Process a single camera feed and publish frames to Kafka
        
        Args:
            cam_id: Camera ID (1-7)
            max_frames: Maximum number of frames to process (None for all)
        """
        camera_file = self.camera_files[cam_id - 1]
        topic_name = self.camera_topics[cam_id - 1]
        
        logger.info(f"Starting to process camera {cam_id} from {camera_file}")
        
        # Check if video file exists
        if not os.path.exists(camera_file):
            logger.error(f"Video file not found: {camera_file}")
            return
        
        # Open video capture
        cap = cv2.VideoCapture(camera_file)
        
        if not cap.isOpened():
            logger.error(f"Failed to open video file: {camera_file}")
            return
        
        try:
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
                        
                        # Optional: Wait for confirmation (comment out for better performance)
                        # future.get(timeout=10)
                        
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
            cap.release()
    
    def start_all_cameras(self, max_frames_per_camera: int = None, use_threading: bool = True):
        """
        Start processing all camera feeds
        
        Args:
            max_frames_per_camera: Maximum frames to process per camera
            use_threading: Whether to use threading for parallel processing
        """
        if not self._connect_kafka():
            logger.error("Failed to connect to Kafka. Exiting.")
            return
        
        # Create topics
        self.create_topics()
        
        logger.info("Starting to process all camera feeds...")
        
        if use_threading:
            # Process cameras in parallel using threads
            threads = []
            
            for cam_id in range(1, 8):
                thread = threading.Thread(
                    target=self.process_camera_feed,
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
                self.process_camera_feed(cam_id, max_frames_per_camera)
        
        # Flush and close producer
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        logger.info("All camera feeds processed successfully!")
    
    def start_single_camera(self, cam_id: int, max_frames: int = None):
        """
        Start processing a single camera feed
        
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
        self.process_camera_feed(cam_id, max_frames)
        
        # Flush and close producer
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        logger.info(f"Camera {cam_id} processing completed!")


def main():
    """Main function to run the producer"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Camera Frame Producer")
    parser.add_argument("--kafka-servers", default="localhost:9092", 
                       help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--camera", type=int, choices=range(1, 8), 
                       help="Process single camera (1-7)")
    parser.add_argument("--max-frames", type=int, 
                       help="Maximum frames to process per camera")
    parser.add_argument("--no-threading", action="store_true", 
                       help="Disable threading for sequential processing")
    
    args = parser.parse_args()
    
    # Initialize producer
    producer = CameraFrameProducer(kafka_bootstrap_servers=args.kafka_servers)
    
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

