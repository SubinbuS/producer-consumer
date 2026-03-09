import os
import shutil
import multiprocessing as mp
import time
from producer_consumer import run_parallel_inversion
import random

MAX_FILES = 10
MAX_PROCESSES = 8
TIMEOUT_SECONDS = 300

SUPPORTED_EXTENSIONS = ['.jpg', '.jpeg', '.png']


def find_all_inputs():
    files = []

    for i in range(1, MAX_FILES + 1):
        found = False
        for ext in SUPPORTED_EXTENSIONS:
            filename = f"input{i}{ext}"
            if os.path.exists(filename):
                files.append(f"input{i}")
                found = True
                break

        if not found:
            print(f"Warning: file input{i} not found(neither .jpg, neither .png).")
    return files


def find_all_inputs_with_extensions():
    files = []
    for i in range(1, MAX_FILES + 1):
        for ext in SUPPORTED_EXTENSIONS:
            filename = f"input{i}{ext}"
            if os.path.exists(filename):
                files.append(f"input{i}{ext}")
                break
    return files


def select_random_subset(all_files, min_count=3, max_count=None):
    if max_count is None:
        max_count = len(all_files)

    count = random.randint(min_count, max_count)
    return random.sample(all_files, min(count, len(all_files)))


def get_files_for_test(test_number, all_images):
    if test_number == 1:
        return select_random_subset(all_images, min_count=2, max_count=4)
    elif test_number == 2:
        return select_random_subset(all_images, min_count=3, max_count=5)
    elif test_number == 3:
        return select_random_subset(all_images, min_count=4, max_count=6)
    elif test_number == 4:
        return select_random_subset(all_images, min_count=5, max_count=8)

    elif test_number == 5:
        return all_images[:3]
    elif test_number == 6:
        return all_images[:5]
    elif test_number == 7:
        return all_images[:7]
    elif test_number == 8:
        return all_images[:min(10, len(all_images))]
    else:
        return all_images[:test_number]


def check_files_exist(image_list):
    missing = []
    for img in image_list:
        found = False
        for ext in SUPPORTED_EXTENSIONS:
            if os.path.exists(f"{img}{ext}"):
                found = True
                break
        if not found:
            missing.append(img)

    if missing:
        print(f"WARNING:Next files not founded: {missing}")
        return False
    return True


def run_single_test(test_number, num_workers, all_images):
    test_folder = f"test{test_number}"

    if os.path.exists(test_folder):
        shutil.rmtree(test_folder)

    os.makedirs(test_folder)

    image_list = get_files_for_test(test_number, all_images)

    if not check_files_exist(image_list):
        print(f"SKIP text {test_number}: files are missing")
        return

    test_type = "RANDOMLY" if test_number <= 4 else "INCREASING"

    print(f"\n{'=' * 60}")
    print(f"Test {test_number} | Processes: {num_workers} | Type: {test_type}")
    print(f"{'=' * 60}")
    print(f"Files are being processed: {len(image_list)}")

    files_with_ext = []
    for img in image_list:
        for ext in SUPPORTED_EXTENSIONS:
            if os.path.exists(f"{img}{ext}"):
                files_with_ext.append(f"{img}{ext}")
                break

    print(f"Files: {files_with_ext}")
    print(f"Count of processes: {num_workers}")

    start_time = time.perf_counter()

    process = mp.Process(
        target=run_parallel_inversion,
        args=(image_list, num_workers)
    )

    process.start()
    process.join(timeout=TIMEOUT_SECONDS)

    if process.is_alive():
        print(f"[PROBLEM] Hanging is possible! Process not complete till {TIMEOUT_SECONDS} s.")
        process.terminate()
        process.join()
        return

    end_time = time.perf_counter()
    total_time = end_time - start_time

    moved_files = 0
    for file in os.listdir():
        if file.startswith("output_"):
            if os.path.isfile(file):
                shutil.move(file, os.path.join(test_folder, file))
                moved_files += 1

    print(f"\nResults of texts: {test_number}:")
    print(f"  - Lead time: {total_time:.3f} сек")
    print(f"  - Processed files: {len(image_list)}")
    print(f"  - Results received: {moved_files}")
    print(f"  - Results saved to folder: {test_folder}")
    if moved_files > 0:
        print(f"  - Average time on file: {total_time / moved_files:.3f} s")


def main():
    mp.freeze_support()

    print("=== Automatic testing (support JPG and PNG) ===")
    print(f"Supported formats: {SUPPORTED_EXTENSIONS}")
    print("Tests 1-4: random sets of files (different sizes)")
    print("Tests 5-8: increadsing count of files (3, 5, 7, all)")
    print()

    all_images = find_all_inputs()

    if not all_images:
        print("Files input1.jpg/input1.png - input10.jpg/input10.png not found.")
        print("Make sure you have files with names input1.jpg, input2.png and etc.")
        return

    print(f"Files was finding (without extension): {all_images}")
    print(f"Всего файлов: {len(all_images)}")

    print("\nFiles with extension:")
    for img in all_images:
        for ext in SUPPORTED_EXTENSIONS:
            if os.path.exists(f"{img}{ext}"):
                print(f"  - {img}{ext}")
                break

    random.seed(42)
    test_counter = 1

    for workers in range(1, MAX_PROCESSES + 1):
        run_single_test(test_counter, workers, all_images)
        test_counter += 1

    print("\n=== All tests complete ===")


if __name__ == "__main__":
    main()