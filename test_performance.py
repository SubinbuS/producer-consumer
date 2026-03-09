import os
import shutil
import multiprocessing as mp
import time
from producer_consumer import run_parallel_inversion


MAX_FILES = 10
TIMEOUT_SECONDS = 300
SUPPORTED_EXTENSIONS = [".png", ".jpeg", ".jpg"]


def find_existing_file(base_name: str):
    for ext in SUPPORTED_EXTENSIONS:
        filename = f"{base_name}{ext}"
        if os.path.exists(filename):
            return filename
    return None


def find_all_inputs():
    found = []

    for i in range(1, MAX_FILES + 1):
        base_name = f"input{i}"
        if find_existing_file(base_name):
            found.append(base_name)

    return found


def get_file_with_extension(base_name: str):
    for ext in SUPPORTED_EXTENSIONS:
        filename = f"{base_name}{ext}"
        if os.path.exists(filename):
            return filename
    return None


def build_test_cases(all_images):
    total = len(all_images)

    def take(n):
        return all_images[:min(n, total)]

    return [
        (1, take(1)),   # 1 image - 1 process
        (1, take(2)),   # 2 images - 1 process
        (3, take(4)),   # 4 images - 3 processes
        (4, take(6)),   # 6 images - 4 processes
        (4, take(3)),   # 3 images - 4 processes
        (4, take(5)),   # 5 images - 4 processes
        (4, take(8)),   # 8 images - 4 processes
        (4, take(10)),  # up to 10 images - 4 processes
    ]


def move_outputs_to_folder(test_folder):
    moved_files = 0

    for file in os.listdir():
        if file.startswith("output_") and os.path.isfile(file):
            shutil.move(file, os.path.join(test_folder, file))
            moved_files += 1

    return moved_files


def run_single_test(test_number, num_workers, image_list):

    test_folder = f"test{test_number}"

    if os.path.exists(test_folder):
        shutil.rmtree(test_folder)

    os.makedirs(test_folder)

    files_with_ext = []
    for img in image_list:
        full_name = get_file_with_extension(img)
        if full_name:
            files_with_ext.append(full_name)

    print("\n" + "=" * 60)
    print(f"Test {test_number} | Processes: {num_workers}")
    print("=" * 60)
    print(f"Images count: {len(image_list)}")
    print(f"Images: {files_with_ext}")

    start_time = time.perf_counter()

    process = mp.Process(
        target=run_parallel_inversion,
        args=(image_list, num_workers)
    )

    process.start()
    process.join(timeout=TIMEOUT_SECONDS)

    if process.is_alive():
        print(f"[ERROR] Timeout: process did not finish within {TIMEOUT_SECONDS} seconds")
        process.terminate()
        process.join()
        return

    end_time = time.perf_counter()
    total_time = end_time - start_time

    moved_files = move_outputs_to_folder(test_folder)

    print("\nResults:")
    print(f"Execution time: {total_time:.3f} sec")
    print(f"Images processed: {len(image_list)}")
    print(f"Outputs saved: {moved_files}")
    print(f"Results folder: {test_folder}")

    if moved_files > 0:
        print(f"Average time per image: {total_time / moved_files:.3f} sec")


def main():

    mp.freeze_support()

    print("=== Automatic Performance Testing ===")
    print(f"Supported formats: {SUPPORTED_EXTENSIONS}")
    print("Test plan:")
    print("1) 1 image  - 1 process")
    print("2) 2 images - 1 process")
    print("3) 4 images - 3 processes")
    print("4) 6 images - 4 processes")
    print("5) 3 images - 4 processes")
    print("6) 5 images - 4 processes")
    print("7) 8 images - 4 processes")
    print("8) up to 10 images - 4 processes")
    print()

    all_images = find_all_inputs()

    if not all_images:
        print("No input files found: input1..input10 with extensions PNG/JPEG/JPG")
        return

    print("Detected images (without extension):", all_images)
    print(f"Total detected images: {len(all_images)}")

    print("\nImages with extensions:")
    for img in all_images:
        full_name = get_file_with_extension(img)
        if full_name:
            print(" ", full_name)

    test_cases = build_test_cases(all_images)

    for test_number, (num_workers, image_list) in enumerate(test_cases, start=1):

        if not image_list:
            print(f"\nTest {test_number} skipped: no images available")
            continue

        run_single_test(test_number, num_workers, image_list)

    print("\n=== All tests completed ===")


if __name__ == "__main__":
    main()
