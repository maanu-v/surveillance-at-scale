#!/usr/bin/env python3

import json
import base64
import cv2
import numpy as np
from kafka import KafkaConsumer
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CameraFrameConsumer:
    def __init__(self, kafka_bootstrap_servers: str = "localhost:9092"):
        """
        Initialize the Kafka consumer for camera frames
        
        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers connection string
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.consumer = None
        self.camera_topics = [f"camera-{i}-frames" for i in range(1, 8)]
    
    def _connect_kafka(self, topics: list):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8'),
                group_id='camera-frame-consumers',
                auto_offset_reset='earliest',  # Start from beginning
                enable_auto_commit=True
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
            # Decode base64 to bytes
            frame_bytes = base64.b64decode(frame_data)
            # Convert bytes to numpy array
            nparr = np.frombuffer(frame_bytes, np.uint8)
            # Decode image
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            return frame
        except Exception as e:
            logger.error(f"Failed to decode frame: {e}")
            return None
    
    def consume_single_camera(self, cam_id: int, max_messages: int = None, display_frames: bool = False):
        """
        Consume frames from a single camera topic
        
        Args:
            cam_id: Camera ID (1-7)
            max_messages: Maximum number of messages to consume
            display_frames: Whether to display frames using OpenCV
        """
        if not (1 <= cam_id <= 7):
            logger.error("Camera ID must be between 1 and 7")
            return
        
        topic = f"camera-{cam_id}-frames"
        
        if not self._connect_kafka([topic]):
            logger.error("Failed to connect to Kafka. Exiting.")
            return
        
        logger.info(f"Starting to consume from camera {cam_id} topic: {topic}")
        
        try:
            message_count = 0
            
            for message in self.consumer:
                try:
                    # Extract message data
                    frame_data = message.value
                    metadata = frame_data.get('metadata', {})
                    frame_base64 = frame_data.get('frame_data')
                    
                    logger.info(f"Received frame - Camera: {metadata.get('camId')}, "
                              f"Frame: {metadata.get('frameId')}, "
                              f"Timestamp: {metadata.get('timestamp')}")
                    
                    if display_frames and frame_base64:
                        # Decode and display frame
                        frame = self._decode_frame(frame_base64)
                        if frame is not None:
                            cv2.imshow(f'Camera {cam_id}', frame)
                            if cv2.waitKey(1) & 0xFF == ord('q'):
                                logger.info("User pressed 'q'. Stopping...")
                                break
                    
                    message_count += 1
                    
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
            if display_frames:
                cv2.destroyAllWindows()
    
    def consume_all_cameras(self, max_messages: int = None):
        """
        Consume frames from all camera topics
        
        Args:
            max_messages: Maximum number of messages to consume
        """
        if not self._connect_kafka(self.camera_topics):
            logger.error("Failed to connect to Kafka. Exiting.")
            return
        
        logger.info(f"Starting to consume from all camera topics: {self.camera_topics}")
        
        try:
            message_count = 0
            
            for message in self.consumer:
                try:
                    # Extract message data
                    frame_data = message.value
                    metadata = frame_data.get('metadata', {})
                    
                    logger.info(f"Topic: {message.topic}, "
                              f"Camera: {metadata.get('camId')}, "
                              f"Frame: {metadata.get('frameId')}, "
                              f"Timestamp: {metadata.get('timestamp')}")
                    
                    message_count += 1
                    
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


def main():
    """Main function to run the consumer"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Camera Frame Consumer")
    parser.add_argument("--kafka-servers", default="localhost:9092", 
                       help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--camera", type=int, choices=range(1, 8), 
                       help="Consume from single camera (1-7)")
    parser.add_argument("--max-messages", type=int, 
                       help="Maximum messages to consume")
    parser.add_argument("--display", action="store_true", 
                       help="Display frames using OpenCV (single camera only)")
    
    args = parser.parse_args()
    
    # Initialize consumer
    consumer = CameraFrameConsumer(kafka_bootstrap_servers=args.kafka_servers)
    
    try:
        if args.camera:
            # Consume from single camera
            consumer.consume_single_camera(args.camera, args.max_messages, args.display)
        else:
            # Consume from all cameras
            consumer.consume_all_cameras(args.max_messages)
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Stopping consumer...")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()