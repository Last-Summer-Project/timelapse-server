import ffmpeg
import logging
from pathlib import Path


def make_timelapse(out_dir: Path, items: list[str], framerate: int = 60) -> bool:
    duration = 1 / framerate
    intput_file = str(out_dir / "input.txt")
    output_file = str(out_dir / "out.mp4")

    with open(intput_file, "wb") as in_file:
        for item in items:
            in_file.write(f"file '{str(out_dir / item)}'\n".encode())
            in_file.write(f"duration {duration}\n".encode())

    try:
        vf = f'fps=fps={framerate}:round=up,scale=1280x720:flags=lanczos'
        (
            ffmpeg
            .input(intput_file, format='concat', safe=0)
            .output(output_file, vcodec='h264_nvenc', preset="fast", crf=0, pix_fmt='yuv420p', vf=vf)
            .run()
        )
    except Exception as e:
        logging.error(f"Unknown error occurred: {e}")
        return False
    return True
