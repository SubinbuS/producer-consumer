import multiprocessing as mp
import numpy as np
import time
import os
from PIL import Image

def find_image_file(name: str):
    possible_extensions = [".jpg", ".jpeg", ".png"]

    if os.path.exists(name):
        return name

    for ext in possible_extensions:
        candidate = name + ext
        if os.path.exists(candidate):
            return candidate

    return None

def invert_image_file(input_path: str, output_path: str) -> float:

    start = time.perf_counter()
    img = Image.open(input_path)

    if img.mode == "RGBA":
        arr = np.array(img, dtype=np.uint8)
        rgb = arr[:, :, :3]
        alpha = arr[:, :, 3:]
        rgb = 255 - rgb
        result = np.concatenate((rgb, alpha), axis=2)
    else:
        img = img.convert("RGB")
        arr = np.array(img, dtype=np.uint8)
        result = 255 - arr

    Image.fromarray(result).save(output_path)

    end = time.perf_counter()
    return end - start

def consumer(file_queue: mp.Queue, result_queue: mp.Queue):
    while True:
        input_path = file_queue.get()

        if input_path is None:
            break

        filename = os.path.basename(input_path)
        name, ext = os.path.splitext(filename)
        output_path = f"output_{name}{ext}"

        try:
            duration = invert_image_file(input_path, output_path)
            result_queue.put((input_path, output_path, duration, None))
        except Exception as e:
            result_queue.put((input_path, None, None, str(e)))

def run_parallel_inversion(image_names: list[str], num_workers: int):

    ctx = mp.get_context("spawn")
    file_queue = ctx.Queue()
    result_queue = ctx.Queue()

    workers = []
    for _ in range(num_workers):
        p = ctx.Process(target=consumer, args=(file_queue, result_queue))
        p.start()
        workers.append(p)

    valid_files = []

    for name in image_names:
        path = find_image_file(name)
        if path:
            file_queue.put(path)
            valid_files.append(path)
        else:
            print(f"[Error] File {name} not found")

    for _ in range(num_workers):
        file_queue.put(None)

    results = []
    for _ in range(len(valid_files)):
        results.append(result_queue.get())

    for p in workers:
        p.join()

    print("\n=== Results ===")
    for in_path, out_path, duration, err in results:
        if err:
            print(f"[FAIL] {in_path}: {err}")
        else:
            print(f"[OK] {in_path} → {out_path} | {duration:.3f} s")

    print("=== Ready ===")

def main():
    mp.freeze_support()

    try:
        num_workers = int(input("Enter count of processes: ").strip())
        if num_workers <= 0:
            raise ValueError
    except ValueError:
        print("Incorrect number of processes.")
        return

    files_input = input(
        "Enter names of files through spaces(with extension or not): "
    ).strip().split()

    if not files_input:
        print("Files not specified.")
        return

    print("\n=== Starting parallel processing ===")

    overall_start = time.perf_counter()

    run_parallel_inversion(files_input, num_workers)

    overall_end = time.perf_counter()

    print(f"\nTotal lead time: {overall_end - overall_start:.3f} s")


if __name__ == "__main__":
    main()