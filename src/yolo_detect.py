# src/yolo_detect.py

import csv
from pathlib import Path
from ultralytics import YOLO

# =========================
# Configuration
# =========================

# Root directory containing channel subfolders with images
IMAGE_DIR = Path("data/raw/images")

# Output CSV file
OUTPUT_CSV = Path("data/processed/yolo_detections.csv")

# YOLO model (nano for efficiency)
MODEL_NAME = "yolov8n.pt"

# Supported image formats
IMAGE_EXTENSIONS = ("*.jpg", "*.jpeg", "*.png", "*.JPG", "*.PNG")

# =========================
# Load YOLO Model
# =========================

model = YOLO(MODEL_NAME)

# =========================
# Helper Functions
# =========================

def classify_image(objects: set) -> str:
    """
    Classify an image based on detected objects.

    Args:
        objects (set): Set of detected object names

    Returns:
        str: Image category
    """
    has_person = "person" in objects
    has_product = bool(objects.intersection({"bottle", "cup", "container"}))

    if has_person and has_product:
        return "promotional"
    elif has_product and not has_person:
        return "product_display"
    elif has_person and not has_product:
        return "lifestyle"
    else:
        return "other"


# =========================
# Core Detection Logic
# =========================

def run_detection():
    """
    Run YOLOv8 object detection on all images in the data lake.

    Returns:
        list: Detection results
    """
    results_data = []

    print(f"🔍 Scanning images in: {IMAGE_DIR.resolve()}")

    if not IMAGE_DIR.exists():
        print("❌ IMAGE_DIR does not exist. Check your path.")
        return results_data

    # Collect all images recursively
    image_paths = []
    for ext in IMAGE_EXTENSIONS:
        image_paths.extend(IMAGE_DIR.rglob(ext))

    if not image_paths:
        print("⚠️ No images found. Check IMAGE_DIR path or image formats.")
        return results_data

    print(f"📸 Found {len(image_paths)} images. Running YOLO detection...")

    for image_path in image_paths:
        detections = model(image_path, verbose=False)[0]

        detected_objects = set()
        confidence_scores = []

        if detections.boxes is not None:
            for box in detections.boxes:
                class_name = model.names[int(box.cls)]
                confidence = float(box.conf)

                detected_objects.add(class_name)
                confidence_scores.append(confidence)

        image_category = classify_image(detected_objects)

        avg_confidence = round(
            sum(confidence_scores) / len(confidence_scores), 3
        ) if confidence_scores else 0.0

        results_data.append([
            image_path.name,
            ",".join(sorted(detected_objects)),
            image_category,
            avg_confidence
        ])

    print("✅ Object detection completed successfully.")
    return results_data


# =========================
# Save Results
# =========================

def save_to_csv(data):
    """
    Save YOLO detection results to CSV.
    """
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "image_name",
            "detected_objects",
            "image_category",
            "confidence_score"
        ])
        writer.writerows(data)

    print(f"📄 YOLO results saved to {OUTPUT_CSV.resolve()}")


# =========================
# Entry Point
# =========================

if __name__ == "__main__":
    results = run_detection()
    save_to_csv(results)
