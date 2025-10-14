#!/bin/bash

# =============================================================================
# Hadoop Parquet Storage Script
# =============================================================================
# This script helps store detection results in HDFS Parquet format
# mimicking the structure created by the Spark YOLO consumer job
#
# Usage:
#   ./hadoop-parquet.sh [operation] [options]
#
# Operations:
#   setup     - Create HDFS directory structure
#   upload    - Upload sample parquet files
#   list      - List files in HDFS surveillance directories
#   clean     - Clean up HDFS surveillance directories
#   test      - Create and upload test data
# =============================================================================

# Configuration
HDFS_BASE_PATH="/surveillance"
HDFS_DETECTIONS_PATH="${HDFS_BASE_PATH}/detections"
HDFS_DETECTIONS_FLATTENED_PATH="${HDFS_BASE_PATH}/detections_flattened"
HDFS_CHECKPOINTS_PATH="${HDFS_BASE_PATH}/checkpoints"
NAMENODE_URL="hdfs://namenode:9000"

# Docker configuration
NAMENODE_CONTAINER="namenode"
DOCKER_NETWORK="newww_surveillance-net"

# Local directories for temporary files
LOCAL_TEMP_DIR="/tmp/hadoop-parquet-temp"
SAMPLE_DATA_DIR="${LOCAL_TEMP_DIR}/sample_data"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# =============================================================================
# Helper Functions
# =============================================================================

print_header() {
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE} $1${NC}"
    echo -e "${BLUE}============================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if HDFS is available via Docker
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker command not found. Make sure Docker is installed and running."
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker daemon."
        exit 1
    fi
    
    print_success "Docker is available and running"
}

check_hdfs() {
    check_docker
    
    # Check if namenode container is running
    if ! docker ps --format "table {{.Names}}" | grep -q "^${NAMENODE_CONTAINER}$"; then
        print_error "Namenode container '${NAMENODE_CONTAINER}' is not running."
        print_info "Start the Hadoop cluster with: docker-compose up -d"
        
        # Show available containers
        echo -e "\n${BLUE}Available containers:${NC}"
        docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
        exit 1
    fi
    
    # Test HDFS connectivity through Docker
    if ! docker exec ${NAMENODE_CONTAINER} hdfs dfs -test -d / 2>/dev/null; then
        print_error "Cannot connect to HDFS through Docker container."
        print_info "Make sure the Hadoop cluster is fully initialized."
        print_info "You can check namenode logs with: docker logs ${NAMENODE_CONTAINER}"
        exit 1
    fi
    
    print_success "HDFS connectivity verified via Docker"
}

show_docker_status() {
    print_header "Docker Hadoop Cluster Status"
    
    check_docker
    
    echo -e "\n${BLUE}Hadoop containers status:${NC}"
    docker ps --filter "name=namenode" --filter "name=datanode" --filter "name=resourcemanager" --filter "name=nodemanager" \
        --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" || print_warning "No Hadoop containers found"
    
    echo -e "\n${BLUE}Namenode Web UI:${NC} http://localhost:9870"
    echo -e "${BLUE}ResourceManager Web UI:${NC} http://localhost:8088"
    
    if docker ps --format "table {{.Names}}" | grep -q "^${NAMENODE_CONTAINER}$"; then
        echo -e "\n${BLUE}HDFS Safe Mode Status:${NC}"
        docker exec ${NAMENODE_CONTAINER} hdfs dfsadmin -safemode get 2>/dev/null || print_warning "Cannot get safe mode status"
        
        echo -e "\n${BLUE}HDFS Disk Usage:${NC}"
        docker exec ${NAMENODE_CONTAINER} hdfs dfs -df -h / 2>/dev/null || print_warning "Cannot get disk usage"
    fi
}

# Execute HDFS command via Docker
hdfs_exec() {
    docker exec ${NAMENODE_CONTAINER} hdfs dfs "$@"
}

# =============================================================================
# HDFS Directory Operations
# =============================================================================

setup_hdfs_directories() {
    print_header "Setting up HDFS directory structure"
    
    check_hdfs
    
    # Create base surveillance directory
    hdfs_exec -mkdir -p ${HDFS_BASE_PATH}
    print_info "Created ${HDFS_BASE_PATH}"
    
    # Create detections directories with partitions (optimized)
    print_info "Creating partitioned directories for detections..."
    for year in 2024 2025; do
        for month in {01..12}; do
            # Create monthly directories in batches to reduce Docker exec calls
            monthly_paths=""
            for day in {01..31}; do
                for camera in {1..7}; do
                    partition_path="${HDFS_DETECTIONS_PATH}/year=${year}/month=${month}/day=${day}/camera_id=${camera}"
                    monthly_paths="${monthly_paths} ${partition_path}"
                done
            done
            # Create all paths for this month at once
            hdfs_exec -mkdir -p ${monthly_paths}
        done
    done
    print_success "Created partitioned directories for detections"
    
    # Create flattened detections directories (optimized)
    print_info "Creating partitioned directories for flattened detections..."
    for year in 2024 2025; do
        for month in {01..12}; do
            # Create monthly directories in batches
            monthly_paths=""
            for day in {01..31}; do
                for camera in {1..7}; do
                    partition_path="${HDFS_DETECTIONS_FLATTENED_PATH}/year=${year}/month=${month}/day=${day}/camera_id=${camera}"
                    monthly_paths="${monthly_paths} ${partition_path}"
                done
            done
            # Create all paths for this month at once
            hdfs_exec -mkdir -p ${monthly_paths}
        done
    done
    print_success "Created partitioned directories for flattened detections"
    
    # Create checkpoint directories
    hdfs_exec -mkdir -p ${HDFS_CHECKPOINTS_PATH}/yolo-detections
    hdfs_exec -mkdir -p ${HDFS_CHECKPOINTS_PATH}/yolo-detections_flattened
    print_info "Created checkpoint directories"
    
    print_success "HDFS directory structure created successfully"
}

# =============================================================================
# Sample Data Generation
# =============================================================================

generate_sample_detection_data() {
    local output_file="$1"
    local camera_id="$2"
    local frame_id="$3"
    
    cat > "${output_file}" << EOF
{
  "camera_id": ${camera_id},
  "frame_id": ${frame_id},
  "timestamp": "$(date -u +"%Y-%m-%d %H:%M:%S.%3N")",
  "detection_timestamp": "$(date -u +"%Y-%m-%d %H:%M:%S.%3N")",
  "total_detections": 2,
  "detection_classes": ["person", "car"],
  "class_counts": "{\"person\": 1, \"car\": 1}",
  "confidence_threshold": 0.4,
  "processing_engine": "spark-yolo",
  "error": null,
  "detections": [
    {
      "class_id": 0,
      "class_name": "person", 
      "confidence": 0.87,
      "bbox_x": 100,
      "bbox_y": 150,
      "bbox_width": 80,
      "bbox_height": 180,
      "bbox_center_x": 140,
      "bbox_center_y": 240
    },
    {
      "class_id": 2,
      "class_name": "car",
      "confidence": 0.92,
      "bbox_x": 300,
      "bbox_y": 200,
      "bbox_width": 120,
      "bbox_height": 60,
      "bbox_center_x": 360,
      "bbox_center_y": 230
    }
  ],
  "ingestion_timestamp": "$(date -u +"%Y-%m-%d %H:%M:%S.%3N")",
  "year": $(date +"%Y"),
  "month": $(date +"%m"),
  "day": $(date +"%d"),
  "hour": $(date +"%H")
}
EOF
}

create_sample_parquet_files() {
    print_header "Creating sample Parquet files"
    
    # Create local temp directory
    mkdir -p "${SAMPLE_DATA_DIR}"
    
    # Generate sample JSON data for different cameras and frames
    for camera_id in {1..3}; do
        for frame_id in {1..5}; do
            json_file="${SAMPLE_DATA_DIR}/camera_${camera_id}_frame_${frame_id}.json"
            generate_sample_detection_data "${json_file}" "${camera_id}" "${frame_id}"
        done
    done
    
    print_success "Generated sample JSON detection data"
    
    # Create a simple Python script to convert JSON to Parquet
    cat > "${SAMPLE_DATA_DIR}/json_to_parquet.py" << 'EOF'
import pandas as pd
import json
import sys
import os
from datetime import datetime

def json_to_parquet(json_dir, output_dir):
    """Convert JSON files to Parquet format"""
    os.makedirs(output_dir, exist_ok=True)
    
    for json_file in os.listdir(json_dir):
        if json_file.endswith('.json'):
            json_path = os.path.join(json_dir, json_file)
            
            with open(json_path, 'r') as f:
                data = json.load(f)
            
            # Convert to DataFrame
            df = pd.json_normalize(data)
            
            # Create parquet filename
            parquet_file = json_file.replace('.json', '.parquet')
            parquet_path = os.path.join(output_dir, parquet_file)
            
            # Save as parquet
            df.to_parquet(parquet_path, index=False)
            print(f"Converted {json_file} -> {parquet_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python json_to_parquet.py <json_dir> <output_dir>")
        sys.exit(1)
    
    json_to_parquet(sys.argv[1], sys.argv[2])
EOF
    
    # Convert JSON to Parquet (requires pandas and pyarrow)
    if command -v python3 &> /dev/null; then
        print_info "Converting JSON files to Parquet format..."
        python3 "${SAMPLE_DATA_DIR}/json_to_parquet.py" "${SAMPLE_DATA_DIR}" "${SAMPLE_DATA_DIR}/parquet"
        print_success "Converted JSON files to Parquet format"
    else
        print_warning "Python3 not found. Skipping Parquet conversion."
        print_info "You can manually convert JSON files to Parquet format later."
    fi
}

# =============================================================================
# Upload Operations
# =============================================================================

upload_sample_files() {
    print_header "Uploading sample files to HDFS"
    
    check_hdfs
    
    if [ ! -d "${SAMPLE_DATA_DIR}/parquet" ]; then
        print_error "No parquet files found. Run 'test' operation first to create sample data."
        exit 1
    fi
    
    # Get current date for partitioning
    current_year=$(date +"%Y")
    current_month=$(date +"%m")
    current_day=$(date +"%d")
    
    # Upload parquet files to appropriate partitions
    for parquet_file in "${SAMPLE_DATA_DIR}/parquet"/*.parquet; do
        if [ -f "${parquet_file}" ]; then
            filename=$(basename "${parquet_file}")
            
            # Extract camera ID from filename
            camera_id=$(echo "${filename}" | sed -n 's/.*camera_\([0-9]*\)_.*/\1/p')
            
            if [ -n "${camera_id}" ]; then
                # Copy file to Docker container first, then move to HDFS
                docker cp "${parquet_file}" "${NAMENODE_CONTAINER}:/tmp/${filename}"
                
                # Upload to partitioned directory
                hdfs_path="${HDFS_DETECTIONS_PATH}/year=${current_year}/month=${current_month}/day=${current_day}/camera_id=${camera_id}/${filename}"
                hdfs_exec -put "/tmp/${filename}" "${hdfs_path}"
                print_success "Uploaded ${filename} to ${hdfs_path}"
                
                # Also upload to flattened directory
                hdfs_path_flat="${HDFS_DETECTIONS_FLATTENED_PATH}/year=${current_year}/month=${current_month}/day=${current_day}/camera_id=${camera_id}/${filename}"
                hdfs_exec -put "/tmp/${filename}" "${hdfs_path_flat}"
                print_success "Uploaded ${filename} to ${hdfs_path_flat}"
                
                # Clean up temporary file in container
                docker exec ${NAMENODE_CONTAINER} rm -f "/tmp/${filename}"
            else
                print_warning "Could not extract camera ID from ${filename}"
            fi
        fi
    done
    
    print_success "Sample files uploaded successfully"
}

upload_custom_file() {
    local local_file="$1"
    local camera_id="$2"
    local target_date="$3"
    
    if [ ! -f "${local_file}" ]; then
        print_error "File ${local_file} not found"
        return 1
    fi
    
    # Parse date (format: YYYY-MM-DD)
    year=$(echo "${target_date}" | cut -d'-' -f1)
    month=$(echo "${target_date}" | cut -d'-' -f2)
    day=$(echo "${target_date}" | cut -d'-' -f3)
    
    filename=$(basename "${local_file}")
    
    # Copy file to Docker container first
    docker cp "${local_file}" "${NAMENODE_CONTAINER}:/tmp/${filename}"
    
    # Upload to both regular and flattened directories
    hdfs_path="${HDFS_DETECTIONS_PATH}/year=${year}/month=${month}/day=${day}/camera_id=${camera_id}/${filename}"
    hdfs_path_flat="${HDFS_DETECTIONS_FLATTENED_PATH}/year=${year}/month=${month}/day=${day}/camera_id=${camera_id}/${filename}"
    
    hdfs_exec -put "/tmp/${filename}" "${hdfs_path}"
    print_success "Uploaded ${filename} to ${hdfs_path}"
    
    hdfs_exec -put "/tmp/${filename}" "${hdfs_path_flat}"
    print_success "Uploaded ${filename} to ${hdfs_path_flat}"
    
    # Clean up temporary file in container
    docker exec ${NAMENODE_CONTAINER} rm -f "/tmp/${filename}"
}

# =============================================================================
# List and Query Operations
# =============================================================================

list_hdfs_contents() {
    print_header "Listing HDFS surveillance directories"
    
    check_hdfs
    
    echo -e "\n${BLUE}Base surveillance directory:${NC}"
    hdfs_exec -ls ${HDFS_BASE_PATH} 2>/dev/null || print_warning "Base directory not found"
    
    echo -e "\n${BLUE}Detection results directory structure:${NC}"
    hdfs_exec -ls ${HDFS_DETECTIONS_PATH} 2>/dev/null || print_warning "Detections directory not found"
    
    echo -e "\n${BLUE}Recent detection files (last 10):${NC}"
    hdfs_exec -ls -R ${HDFS_DETECTIONS_PATH} 2>/dev/null | grep "\.parquet$" | tail -10 || print_warning "No parquet files found"
    
    echo -e "\n${BLUE}Flattened detection results:${NC}"
    hdfs_exec -ls ${HDFS_DETECTIONS_FLATTENED_PATH} 2>/dev/null || print_warning "Flattened detections directory not found"
    
    echo -e "\n${BLUE}Checkpoint directories:${NC}"
    hdfs_exec -ls ${HDFS_CHECKPOINTS_PATH} 2>/dev/null || print_warning "Checkpoints directory not found"
}

get_directory_stats() {
    print_header "HDFS Directory Statistics"
    
    check_hdfs
    
    echo -e "\n${BLUE}Disk usage statistics:${NC}"
    hdfs_exec -du -h ${HDFS_BASE_PATH} 2>/dev/null || print_warning "Cannot get disk usage"
    
    echo -e "\n${BLUE}File count by directory:${NC}"
    for dir in "detections" "detections_flattened" "checkpoints"; do
        path="${HDFS_BASE_PATH}/${dir}"
        count=$(hdfs_exec -find ${path} -name "*.parquet" 2>/dev/null | wc -l)
        echo -e "${BLUE}${dir}:${NC} ${count} parquet files"
    done
}

# =============================================================================
# Cleanup Operations
# =============================================================================

clean_hdfs_directories() {
    print_header "Cleaning HDFS surveillance directories"
    
    check_hdfs
    
    read -p "This will delete all surveillance data in HDFS. Are you sure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        hdfs_exec -rm -r ${HDFS_BASE_PATH} 2>/dev/null || print_warning "Directory already clean or not found"
        print_success "HDFS surveillance directories cleaned"
    else
        print_info "Cleanup cancelled"
    fi
}

clean_local_temp() {
    print_header "Cleaning local temporary files"
    
    if [ -d "${LOCAL_TEMP_DIR}" ]; then
        rm -rf "${LOCAL_TEMP_DIR}"
        print_success "Local temporary files cleaned"
    else
        print_info "No local temporary files to clean"
    fi
}

# =============================================================================
# Main Script Logic
# =============================================================================

show_usage() {
    echo -e "${BLUE}Hadoop Parquet Storage Script${NC}"
    echo -e "Usage: $0 [operation] [options]"
    echo -e ""
    echo -e "${BLUE}Operations:${NC}"
    echo -e "  ${GREEN}setup${NC}                     - Create HDFS directory structure"
    echo -e "  ${GREEN}upload${NC}                    - Upload sample parquet files"
    echo -e "  ${GREEN}upload-file${NC} <file> <cam> <date> - Upload specific file to camera partition"
    echo -e "  ${GREEN}list${NC}                      - List files in HDFS surveillance directories"
    echo -e "  ${GREEN}stats${NC}                     - Show directory statistics"
    echo -e "  ${GREEN}status${NC}                    - Show Docker Hadoop cluster status"
    echo -e "  ${GREEN}clean${NC}                     - Clean up HDFS surveillance directories"
    echo -e "  ${GREEN}clean-local${NC}               - Clean up local temporary files"
    echo -e "  ${GREEN}test${NC}                      - Create and upload test data"
    echo -e ""
    echo -e "${BLUE}Docker Requirements:${NC}"
    echo -e "  - Docker and docker-compose must be installed"
    echo -e "  - Hadoop cluster must be running (docker-compose up -d)"
    echo -e "  - Namenode container must be accessible"
    echo -e ""
    echo -e "${BLUE}Examples:${NC}"
    echo -e "  $0 status                           # Check Docker Hadoop status"
    echo -e "  $0 setup                            # Create HDFS directories"
    echo -e "  $0 test                             # Full test with sample data"
    echo -e "  $0 upload-file /path/to/data.parquet 1 2025-10-09"
    echo -e "  $0 list                             # List HDFS contents"
    echo -e "  $0 clean                            # Clean up HDFS"
}

main() {
    local operation="${1:-help}"
    
    # Show script header with Docker info (except for help)
    if [[ "$operation" != "help" && "$operation" != "-h" && "$operation" != "--help" ]]; then
        print_header "Hadoop Parquet Storage Script (Docker Mode)"
        print_info "Namenode container: ${NAMENODE_CONTAINER}"
        print_info "Docker network: ${DOCKER_NETWORK}"
        echo ""
    fi
    
    case $operation in
        "setup")
            setup_hdfs_directories
            ;;
        "upload")
            upload_sample_files
            ;;
        "upload-file")
            if [ $# -ne 4 ]; then
                print_error "Usage: $0 upload-file <local_file> <camera_id> <date>"
                print_error "Example: $0 upload-file /path/to/data.parquet 1 2025-10-09"
                exit 1
            fi
            upload_custom_file "$2" "$3" "$4"
            ;;
        "list")
            list_hdfs_contents
            ;;
        "stats")
            get_directory_stats
            ;;
        "status"|"docker-status")
            show_docker_status
            ;;
        "clean")
            clean_hdfs_directories
            ;;
        "clean-local")
            clean_local_temp
            ;;
        "test")
            create_sample_parquet_files
            setup_hdfs_directories
            upload_sample_files
            list_hdfs_contents
            ;;
        "help"|"-h"|"--help")
            show_usage
            ;;
        *)
            print_error "Unknown operation: $operation"
            show_usage
            exit 1
            ;;
    esac
}

# =============================================================================
# Script Entry Point
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
