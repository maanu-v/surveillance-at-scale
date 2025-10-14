#!/usr/bin/env python3

import os
import json
import base64
import cv2
import numpy as np
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import asyncio
# import asyncpg  # Disabled for testing
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import uvicorn
from ultralytics import YOLO
from deep_sort_realtime.deepsort_tracker import DeepSort

# -----------------------------
# Logging configuration
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------
# Pydantic models for API
# -----------------------------
class FrameData(BaseModel):
    frame_data: str  # base64 encoded frame
    frame_id: int
    timestamp: str
    camera_id: int
    metadata: Optional[Dict[str, Any]] = {}

class BatchFrameData(BaseModel):
    frames: List[FrameData]

class DetectionResult(BaseModel):
    x1: float
    y1: float
    x2: float
    y2: float
    confidence: float
    class_id: int
    class_name: str

class TrackResult(BaseModel):
    track_id: int
    detection: DetectionResult
    embedding: Optional[List[float]] = None
    is_confirmed: bool

class ProcessingResponse(BaseModel):
    camera_id: int
    frame_id: int
    timestamp: str
    processing_time_ms: float
    detections_count: int
    tracks_count: int
    tracks: List[TrackResult]

class ServiceStatus(BaseModel):
    camera_id: int
    status: str
    model_loaded: bool
    tracker_active: bool
    db_connected: bool
    total_frames_processed: int
    total_tracks_created: int
    uptime_seconds: float

# -----------------------------
# YOLO+DeepSORT Microservice
# -----------------------------
class YOLODeepSortMicroservice:
    def __init__(self, camera_id: int, config: Dict[str, Any]):
        self.camera_id = camera_id
        self.config = config
        self.start_time = datetime.now()
        
        # Statistics
        self.stats = {
            "total_frames_processed": 0,
            "total_tracks_created": 0,
            "total_detections": 0,
            "avg_processing_time_ms": 0.0
        }
        
        # YOLO model
        self.model = None
        self.model_loaded = False
        
        # DeepSORT tracker
        self.tracker = None
        self.tracker_active = False
        
        # Database disabled for testing
        # self.db_config = {
        #     'host': os.getenv('DB_HOST', 'localhost'),
        #     'port': int(os.getenv('DB_PORT', 5432)),
        #     'database': os.getenv('DB_NAME', 'tracking'),
        #     'user': os.getenv('DB_USER', 'postgres'),
        #     'password': os.getenv('DB_PASSWORD', 'postgres')
        # }
        # self.db_pool = None
        self.db_pool = None
        
        # Class names (COCO dataset)
        self.class_names = [
            "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train", "truck", "boat",
            "traffic light", "fire hydrant", "stop sign", "parking meter", "bench", "bird", "cat",
            "dog", "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe", "backpack",
            "umbrella", "handbag", "tie", "suitcase", "frisbee", "skis", "snowboard", "sports ball",
            "kite", "baseball bat", "baseball glove", "skateboard", "surfboard", "tennis racket",
            "bottle", "wine glass", "cup", "fork", "knife", "spoon", "bowl", "banana", "apple",
            "sandwich", "orange", "broccoli", "carrot", "hot dog", "pizza", "donut", "cake", "chair",
            "couch", "potted plant", "bed", "dining table", "toilet", "tv", "laptop", "mouse",
            "remote", "keyboard", "cell phone", "microwave", "oven", "toaster", "sink", "refrigerator",
            "book", "clock", "vase", "scissors", "teddy bear", "hair drier", "toothbrush"
        ]

    async def initialize(self):
        """Initialize all components"""
        try:
            # Initialize YOLO model
            logger.info(f"Loading YOLO model for camera {self.camera_id}...")
            model_path = self.config.get("model_path", "yolov8n.pt")
            self.model = YOLO(model_path)
            self.model_loaded = True
            logger.info(f"✅ YOLO model loaded for camera {self.camera_id}")
            
            # Initialize DeepSORT tracker
            logger.info(f"Initializing DeepSORT tracker for camera {self.camera_id}...")
            self.tracker = DeepSort(
                max_age=self.config.get("max_age", 30),
                n_init=self.config.get("n_init", 3),
                nms_max_overlap=self.config.get("nms_max_overlap", 1.0),
                max_cosine_distance=self.config.get("max_cosine_distance", 0.2)
            )
            self.tracker_active = True
            logger.info(f"✅ DeepSORT tracker initialized for camera {self.camera_id}")
            
            # Initialize database connection - DISABLED FOR TESTING
            # await self._init_database()
            logger.info(f"📊 Database disabled for testing - camera {self.camera_id}")
            
            logger.info(f"🚀 Camera {self.camera_id} microservice fully initialized")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize camera {self.camera_id} service: {e}")
            raise

    async def _init_database(self):
        """Initialize PostgreSQL connection pool - DISABLED FOR TESTING"""
        logger.info(f"📊 Database initialization disabled for testing - camera {self.camera_id}")
        self.db_connected = False
        # try:
        #     db_config = self.config["database"]
        #     self.db_pool = await asyncpg.create_pool(
        #         host=db_config["host"],
        #         port=db_config["port"],
        #         user=db_config["user"],
        #         password=db_config["password"],
        #         database=db_config["database"],
        #         min_size=2,
        #         max_size=10
        #     )
        #     self.db_connected = True
        #     logger.info(f"✅ Database connection pool created for camera {self.camera_id}")
        #     
        #     # Create tables if they don't exist
        #     await self._create_tables()
        #     
        # except Exception as e:
        #     logger.error(f"❌ Failed to connect to database for camera {self.camera_id}: {e}")
        #     self.db_connected = False
        #     raise

    async def _create_tables(self):
        """Create necessary database tables"""
        async with self.db_pool.acquire() as conn:
            # Create tracks table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tracks (
                    id SERIAL PRIMARY KEY,
                    camera_id INTEGER NOT NULL,
                    frame_id INTEGER NOT NULL,
                    track_id INTEGER NOT NULL,
                    class_id INTEGER NOT NULL,
                    class_name VARCHAR(50) NOT NULL,
                    confidence REAL NOT NULL,
                    x1 REAL NOT NULL,
                    y1 REAL NOT NULL,
                    x2 REAL NOT NULL,
                    y2 REAL NOT NULL,
                    embedding REAL[],
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create index for faster queries
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tracks_camera_frame 
                ON tracks (camera_id, frame_id);
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tracks_timestamp 
                ON tracks (timestamp);
            """)

    def _decode_frame(self, frame_b64: str) -> np.ndarray:
        """Decode base64-encoded frame to numpy array"""
        try:
            frame_bytes = base64.b64decode(frame_b64)
            nparr = np.frombuffer(frame_bytes, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if frame is None:
                raise ValueError("Failed to decode frame")
            return frame
        except Exception as e:
            logger.error(f"Failed to decode frame: {e}")
            raise

    async def process_frame(self, frame_data: FrameData) -> ProcessingResponse:
        """Process a single frame through YOLO + DeepSORT pipeline"""
        start_time = datetime.now()
        
        try:
            # Decode frame
            frame = self._decode_frame(frame_data.frame_data)
            
            # YOLO inference
            results = self.model(frame, verbose=False)[0]
            detections = []
            
            if results.boxes is not None:
                for box in results.boxes:
                    x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                    confidence = box.conf[0].cpu().numpy()
                    class_id = int(box.cls[0].cpu().numpy())
                    
                    # Filter by confidence threshold
                    if confidence >= self.config.get("confidence_threshold", 0.5):
                        detections.append([x1, y1, x2, y2, confidence, class_id])
            
            # DeepSORT tracking
            tracks = []
            if detections:
                track_results = self.tracker.update_tracks(detections, frame=frame)
                
                for track in track_results:
                    if track.is_confirmed():
                        bbox = track.to_ltrb()  # [x1, y1, x2, y2]
                        
                        track_result = TrackResult(
                            track_id=track.track_id,
                            detection=DetectionResult(
                                x1=float(bbox[0]),
                                y1=float(bbox[1]),
                                x2=float(bbox[2]),
                                y2=float(bbox[3]),
                                confidence=float(track.det_conf) if hasattr(track, 'det_conf') else 0.0,
                                class_id=int(track.det_class) if hasattr(track, 'det_class') else 0,
                                class_name=self.class_names[int(track.det_class)] if hasattr(track, 'det_class') and track.det_class < len(self.class_names) else "unknown"
                            ),
                            embedding=track.embedding.tolist() if hasattr(track, 'embedding') and track.embedding is not None else None,
                            is_confirmed=track.is_confirmed()
                        )
                        tracks.append(track_result)
            
            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Update statistics
            self.stats["total_frames_processed"] += 1
            self.stats["total_detections"] += len(detections)
            self.stats["total_tracks_created"] += len(tracks)
            
            # Update average processing time
            frames_processed = self.stats["total_frames_processed"]
            current_avg = self.stats["avg_processing_time_ms"]
            self.stats["avg_processing_time_ms"] = (
                (current_avg * (frames_processed - 1) + processing_time) / frames_processed
            )
            
            # Prepare response
            response = ProcessingResponse(
                camera_id=self.camera_id,
                frame_id=frame_data.frame_id,
                timestamp=datetime.now().isoformat(),
                processing_time_ms=processing_time,
                detections_count=len(detections),
                tracks_count=len(tracks),
                tracks=tracks
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Error processing frame {frame_data.frame_id} for camera {self.camera_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Frame processing failed: {str(e)}")

    async def save_tracks_to_db(self, tracks: List[TrackResult], frame_id: int):
        """Save tracks to PostgreSQL database - DISABLED FOR TESTING"""
        if not tracks:
            return
        
        logger.debug(f"📊 Would save {len(tracks)} tracks to database (disabled for testing)")
        # Database saving disabled for testing
        # if not tracks or not self.db_connected:
        #     return
        # 
        # try:
        #     async with self.db_pool.acquire() as conn:
        #         query = """
        #             INSERT INTO tracks (
        #                 camera_id, frame_id, track_id, class_id, class_name, 
        #                 confidence, x1, y1, x2, y2, embedding, timestamp
        #             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        #         """
        #         
        #         for track in tracks:
        #             await conn.execute(
        #                 query,
        #                 self.camera_id,
        #                 frame_id,
        #                 track.track_id,
        #                 track.detection.class_id,
        #                 track.detection.class_name,
        #                 track.detection.confidence,
        #                 track.detection.x1,
        #                 track.detection.y1,
        #                 track.detection.x2,
        #                 track.detection.y2,
        #                 track.embedding,
        #                 datetime.now()
        #             )
        #                    
        #     logger.debug(f"Saved {len(tracks)} tracks to database for camera {self.camera_id}, frame {frame_id}")
        #     
        # except Exception as e:
        #     logger.error(f"Failed to save tracks to database: {e}")

    def get_status(self) -> ServiceStatus:
        """Get service status"""
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        return ServiceStatus(
            camera_id=self.camera_id,
            status="running" if self.model_loaded and self.tracker_active else "initializing",
            model_loaded=self.model_loaded,
            tracker_active=self.tracker_active,
            db_connected=self.db_connected,
            total_frames_processed=self.stats["total_frames_processed"],
            total_tracks_created=self.stats["total_tracks_created"],
            uptime_seconds=uptime
        )

# -----------------------------
# Global service instance
# -----------------------------
service: Optional[YOLODeepSortMicroservice] = None

# -----------------------------
# FastAPI application
# -----------------------------
app = FastAPI(
    title="YOLO+DeepSORT Microservice",
    description="Multi-camera object detection and tracking microservice",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    """Initialize service on startup"""
    global service
    
    # Load configuration (this will be passed via environment or config file)
    camera_id = int(os.getenv("CAMERA_ID", "1"))
    
    config = {
        "model_path": os.getenv("YOLO_MODEL_PATH", "yolov8n.pt"),
        "confidence_threshold": float(os.getenv("CONFIDENCE_THRESHOLD", "0.5")),
        "max_age": int(os.getenv("TRACKER_MAX_AGE", "30")),
        "n_init": int(os.getenv("TRACKER_N_INIT", "3")),
        "nms_max_overlap": float(os.getenv("NMS_MAX_OVERLAP", "1.0")),
        "max_cosine_distance": float(os.getenv("MAX_COSINE_DISTANCE", "0.2")),
        "database": {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
            "database": os.getenv("POSTGRES_DATABASE", "tracking")
        }
    }
    
    service = YOLODeepSortMicroservice(camera_id, config)
    await service.initialize()

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "camera_id": service.camera_id if service else None,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/status", response_model=ServiceStatus)
async def get_status():
    """Get service status"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return service.get_status()

@app.post("/process", response_model=ProcessingResponse)
async def process_frame(frame_data: FrameData, background_tasks: BackgroundTasks):
    """Process a single frame"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    # Process frame
    response = await service.process_frame(frame_data)
    
    # Save tracks to database in background
    if response.tracks:
        background_tasks.add_task(
            service.save_tracks_to_db, 
            response.tracks, 
            frame_data.frame_id
        )
    
    return response

@app.post("/process/batch")
async def process_batch(batch_data: BatchFrameData, background_tasks: BackgroundTasks):
    """Process multiple frames in batch"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    results = []
    for frame_data in batch_data.frames:
        try:
            response = await service.process_frame(frame_data)
            results.append(response)
            
            # Save tracks to database in background
            if response.tracks:
                background_tasks.add_task(
                    service.save_tracks_to_db, 
                    response.tracks, 
                    frame_data.frame_id
                )
                
        except Exception as e:
            logger.error(f"Failed to process frame {frame_data.frame_id}: {e}")
            results.append({
                "error": str(e),
                "frame_id": frame_data.frame_id,
                "camera_id": service.camera_id
            })
    
    return {
        "results": results,
        "total_frames": len(batch_data.frames),
        "successful_frames": len([r for r in results if not isinstance(r, dict) or "error" not in r])
    }

@app.get("/stats")
async def get_statistics():
    """Get processing statistics"""
    if not service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    return {
        "camera_id": service.camera_id,
        "statistics": service.stats,
        "uptime_seconds": (datetime.now() - service.start_time).total_seconds()
    }

# -----------------------------
# Main entry point
# -----------------------------
def main():
    """Run the microservice"""
    import argparse
    
    parser = argparse.ArgumentParser(description="YOLO+DeepSORT Microservice")
    parser.add_argument("--camera-id", type=int, required=True, help="Camera ID (1-7)")
    parser.add_argument("--port", type=int, default=8000, help="Service port")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Service host")
    args = parser.parse_args()
    
    # Set environment variables for camera ID
    os.environ["CAMERA_ID"] = str(args.camera_id)
    
    # Run the service
    uvicorn.run(
        "yolods_microservice:app",
        host=args.host,
        port=args.port,
        log_level="info",
        reload=False
    )

if __name__ == "__main__":
    main()