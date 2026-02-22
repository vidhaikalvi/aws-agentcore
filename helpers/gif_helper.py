def pngs_to_gif(
    dirname, output_path="animation.gif", fps=5, loop=0, pattern="*.png", show=True
):
    """
    Create a GIF from PNGs in a directory and display it in a Jupyter notebook.

    Args:
        dirname (str): directory containing png files.
        output_path (str): path to write the gif.
        fps (int): frames per second.
        loop (int): number of loops (0 = infinite).
        pattern (str): glob pattern for images.
        show (bool): if True display the GIF in the notebook.

    Returns:
        str: path to the generated gif.
    """
    import os
    import glob
    from PIL import Image
    from IPython.display import display, Image as IPyImage

    files = sorted(glob.glob(os.path.join(dirname, pattern)))
    if not files:
        raise ValueError(f"No files found in {dirname} matching {pattern}")

    frames = [Image.open(f).convert("RGBA") for f in files]
    duration = int(1000 / max(1, fps))  # ms per frame

    # Save GIF
    frames[0].save(
        output_path,
        save_all=True,
        append_images=frames[1:],
        duration=duration,
        loop=loop,
        optimize=True,
    )

    if show:
        display(IPyImage(filename=output_path))

    return output_path
