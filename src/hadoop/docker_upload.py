#!/usr/bin/env python3

import os
import logging
import subprocess
import shutil
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DockerHDFSUploader:
    def __init__(self, container_name: str = "namenode"):
        """
        Initialize Docker-based HDFS uploader
        
        Args:
            container_name: Name of the HDFS namenode container
        """
        self.container_name = container_name
        self.local_data_path = "data/raw/Wildtrack/"
        self.hdfs_base_path = "/surveillance/camera-feeds"
        
    def check_container_running(self) -> bool:
        """Check if the HDFS namenode container is running"""
        try:
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", self.container_name],
                capture_output=True, text=True, timeout=10
            )
            
            if result.returncode == 0 and result.stdout.strip() == "true":
                logger.info(f"✅ Container {self.container_name} is running")
                return True
            else:
                logger.error(f"❌ Container {self.container_name} is not running")
                return False
        except Exception as e:
            logger.error(f"❌ Error checking container status: {e}")
            return False
    
    def run_hdfs_command(self, command: list) -> bool:
        """
        Run HDFS command inside the container
        
        Args:
            command: List of command arguments
            
        Returns:
            True if successful, False otherwise
        """
        try:
            full_command = ["docker", "exec", self.container_name] + command
            logger.info(f"Running: {' '.join(full_command)}")
            
            result = subprocess.run(full_command, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                logger.info(f"✅ Command successful")
                if result.stdout.strip():
                    logger.info(f"Output: {result.stdout.strip()}")
                return True
            else:
                logger.error(f"❌ Command failed: {result.stderr.strip()}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Command timeout")
            return False
        except Exception as e:
            logger.error(f"❌ Error running command: {e}")
            return False
    
    def create_hdfs_directory(self) -> bool:
        """Create HDFS directory for camera feeds"""
        return self.run_hdfs_command([
            "hdfs", "dfs", "-mkdir", "-p", self.hdfs_base_path
        ])
    
    def copy_file_to_container(self, local_path: str, container_path: str) -> bool:
        """
        Copy file from host to container
        
        Args:
            local_path: Local file path
            container_path: Path inside container
            
        Returns:
            True if successful, False otherwise
        """
        try:
            command = ["docker", "cp", local_path, f"{self.container_name}:{container_path}"]
            logger.info(f"Copying {local_path} to container...")
            
            result = subprocess.run(command, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info(f"✅ File copied to container")
                return True
            else:
                logger.error(f"❌ Copy failed: {result.stderr.strip()}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Copy timeout")
            return False
        except Exception as e:
            logger.error(f"❌ Error copying file: {e}")
            return False
    
    def upload_file_to_hdfs(self, container_path: str, hdfs_path: str) -> bool:
        """
        Upload file from container to HDFS
        
        Args:
            container_path: Path inside container
            hdfs_path: HDFS destination path
            
        Returns:
            True if successful, False otherwise
        """
        return self.run_hdfs_command([
            "hdfs", "dfs", "-put", "-f", container_path, hdfs_path
        ])
    
    def remove_container_file(self, container_path: str) -> bool:
        """Remove temporary file from container"""
        return self.run_hdfs_command(["rm", "-f", container_path])
    
    def upload_camera_video(self, cam_id: int, force_upload: bool = False) -> bool:
        """
        Upload a single camera video to HDFS
        
        Args:
            cam_id: Camera ID (1-7)
            force_upload: Force upload even if file exists
            
        Returns:
            True if successful, False otherwise
        """
        local_file = f"{self.local_data_path}cam{cam_id}.mp4"
        hdfs_file = f"{self.hdfs_base_path}/cam{cam_id}.mp4"
        container_temp_file = f"/tmp/cam{cam_id}.mp4"
        
        # Check if local file exists
        if not os.path.exists(local_file):
            logger.error(f"Local file not found: {local_file}")
            return False
        
        file_size = os.path.getsize(local_file)
        logger.info(f"Uploading {local_file} ({file_size / (1024*1024):.2f} MB) to HDFS")
        
        # Check if file already exists in HDFS
        if not force_upload:
            check_result = subprocess.run([
                "docker", "exec", self.container_name, 
                "hdfs", "dfs", "-test", "-e", hdfs_file
            ], capture_output=True)
            
            if check_result.returncode == 0:
                logger.info(f"File already exists in HDFS: {hdfs_file}")
                return True
        
        try:
            # Step 1: Copy file to container
            if not self.copy_file_to_container(local_file, container_temp_file):
                return False
            
            # Step 2: Upload to HDFS
            if not self.upload_file_to_hdfs(container_temp_file, hdfs_file):
                return False
            
            # Step 3: Clean up temporary file
            self.remove_container_file(container_temp_file)
            
            logger.info(f"✅ Successfully uploaded cam{cam_id}.mp4 to HDFS")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error uploading cam{cam_id}.mp4: {e}")
            # Clean up on error
            self.remove_container_file(container_temp_file)
            return False
    
    def upload_all_camera_videos(self, force_upload: bool = False) -> bool:
        """
        Upload all camera videos to HDFS
        
        Args:
            force_upload: Force upload even if files exist
            
        Returns:
            True if all uploads successful, False otherwise
        """
        # Check if container is running
        if not self.check_container_running():
            logger.error("HDFS container is not running")
            return False
        
        # Create HDFS directory
        if not self.create_hdfs_directory():
            logger.error("Failed to create HDFS directory")
            return False
        
        # Upload each camera video
        success_count = 0
        total_cameras = 7
        
        for cam_id in range(1, 8):
            if self.upload_camera_video(cam_id, force_upload):
                success_count += 1
            else:
                logger.error(f"Failed to upload camera {cam_id}")
        
        logger.info(f"Upload completed: {success_count}/{total_cameras} files successful")
        return success_count == total_cameras
    
    def verify_uploads(self) -> bool:
        """Verify all camera videos are in HDFS"""
        logger.info("Verifying camera video uploads...")
        
        missing_files = []
        for cam_id in range(1, 8):
            hdfs_file = f"{self.hdfs_base_path}/cam{cam_id}.mp4"
            
            check_result = subprocess.run([
                "docker", "exec", self.container_name, 
                "hdfs", "dfs", "-test", "-e", hdfs_file
            ], capture_output=True)
            
            if check_result.returncode != 0:
                missing_files.append(hdfs_file)
        
        if missing_files:
            logger.error(f"Missing files in HDFS: {missing_files}")
            return False
        else:
            logger.info("✅ All camera videos verified in HDFS")
            return True
    
    def list_hdfs_files(self) -> bool:
        """List files in HDFS camera feeds directory"""
        return self.run_hdfs_command([
            "hdfs", "dfs", "-ls", self.hdfs_base_path
        ])


def main():
    """Main function to upload camera videos to HDFS"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Upload camera videos to HDFS using Docker")
    parser.add_argument("--container", default="namenode", 
                       help="HDFS namenode container name (default: namenode)")
    parser.add_argument("--force", action="store_true", 
                       help="Force upload even if files exist")
    parser.add_argument("--verify-only", action="store_true", 
                       help="Only verify uploads, don't upload")
    parser.add_argument("--list-files", action="store_true", 
                       help="List files in HDFS directory")
    
    args = parser.parse_args()
    
    # Initialize uploader
    uploader = DockerHDFSUploader(args.container)
    
    try:
        if args.list_files:
            # List files in HDFS
            uploader.list_hdfs_files()
        elif args.verify_only:
            # Only verify uploads
            if uploader.verify_uploads():
                logger.info("✅ All camera videos are present in HDFS")
            else:
                logger.error("❌ Some camera videos are missing from HDFS")
                exit(1)
        else:
            # Upload camera videos
            if uploader.upload_all_camera_videos(args.force):
                logger.info("✅ All camera videos uploaded successfully")
                
                # Verify uploads
                if uploader.verify_uploads():
                    logger.info("✅ Upload verification successful")
                else:
                    logger.error("❌ Upload verification failed")
                    exit(1)
            else:
                logger.error("❌ Camera video upload failed")
                exit(1)
                
    except KeyboardInterrupt:
        logger.info("Upload interrupted by user")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()