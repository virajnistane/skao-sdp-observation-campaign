#!/usr/bin/env python3
# Author: Viraj Nistane
# Description: This file contains utility functions for plotting and previewing images.

from os import PathLike
from pathlib import Path
from typing import TypeAlias

import numpy as np

from sdp_control.config import config
from sdp_control.models import Observation

FitsPath: TypeAlias = str | PathLike[str]


def _astronomical_normalize(
    image: np.ndarray,
    background_sigma: float = 2.0,
    source_sigma: float = 8.0,
    asinh_scale: float = 6.0,
) -> np.ndarray:
    """Normalize an image using MAD noise estimation and an asinh stretch.

    This asymmetric stretch gives faint positive sources more contrast while
    preventing bright sources from dominating the displayed image.
    """
    valid_mask = np.isfinite(image) & (np.abs(image) < 1e30)
    if not np.any(valid_mask):
        return np.zeros_like(image, dtype=np.float32)

    values = image[valid_mask].astype(np.float64, copy=False)
    median = float(np.nanmedian(values))
    mad = float(np.nanmedian(np.abs(values - median)))
    sigma = 1.4826 * mad

    if not np.isfinite(sigma) or sigma <= 0:
        sigma = float(np.nanstd(values))
    if not np.isfinite(sigma) or sigma <= 0:
        return np.zeros_like(image, dtype=np.float32)

    lower = median - background_sigma * sigma
    upper = median + source_sigma * sigma
    if upper <= lower:
        return np.zeros_like(image, dtype=np.float32)

    clipped = np.clip(np.asarray(image, dtype=np.float64), lower, upper)
    normalized = (clipped - lower) / (upper - lower)
    stretched = np.arcsinh(asinh_scale * normalized) / np.arcsinh(asinh_scale)
    stretched[~valid_mask] = 0.0
    return np.nan_to_num(stretched, nan=0.0, posinf=1.0, neginf=0.0).astype(np.float32)


def fits2png(
    fits_file: str | Path | list[Path] | list[str],
    vmin: float = 0.0,
    vmax: float = 1.0,
    observation: Observation | None = None,
    interactive_mode: bool = False,
) -> None:
    """Plot a FITS image with normalization and color scaling.

    Args:
        fits_file (str | Path | list[Path | str]): Path(s) to the FITS file(s) to be plotted.
        vmin (float): Minimum value for color scaling.
        vmax (float): Maximum value for color scaling.
        observation (Observation | None): The observation object.
        interactive_mode (bool): Whether to display the plot interactively.
    """
    import matplotlib

    if not interactive_mode:
        # Force the non-interactive Agg backend before pyplot/pylab pick a GUI backend.
        # Tk/Qt/etc GUI backends only work on the main thread; this task can run on a
        # Prefect worker thread, where a GUI backend crashes the whole process.
        matplotlib.use("Agg")

    import pylab  # type: ignore[import-untyped]

    if not interactive_mode:
        pylab.ioff()  # Turn off interactive mode to prevent GUI windows from popping up

    import matplotlib.pyplot as plt
    from astropy.io import fits  # type: ignore[import-untyped]

    if isinstance(fits_file, list):
        # If a list of FITS files is provided, process each one
        file_paths: list[Path] = [Path(item) for item in fits_file]
        fig, ax = plt.subplots(
            1, len(fits_file), figsize=(len(fits_file) * 8, 8), squeeze=False
        )
        axes = ax[0]  # Flatten the axes array for easier indexing

        for i, file in enumerate(file_paths):

            with fits.open(file) as hdul:
                image_data = hdul[0].data
                normalized_image = _astronomical_normalize(image_data)

            axes[i].imshow(
                normalized_image[0, 0, :, :],
                origin="lower",
                cmap="gray",
                vmin=vmin,
                vmax=vmax,
            )
            axes[i].set_xlabel("X")
            axes[i].set_ylabel("Y")
            observation_id_text = f" (ID: {observation.id})" if observation else ""
            axes[i].set_title(f"{Path(file).stem}{observation_id_text}")
        fig.savefig(
            fname=str(file_paths[0].parent / "previews.png"), bbox_inches="tight"
        )
    else:
        # If a single FITS file is provided, process it
        with fits.open(fits_file) as hdul:
            image_data = hdul[0].data
            normalized_image = _astronomical_normalize(image_data)

        fig, ax = plt.subplots(1, 1, figsize=(8, 8), squeeze=False)
        ax[0].imshow(
            normalized_image[0, 0, :, :],
            origin="lower",
            cmap="gray",
            vmin=vmin,
            vmax=vmax,
        )
        ax[0].set_xlabel("X")
        ax[0].set_ylabel("Y")
        observation_id_text = f" (ID: {observation.id})" if observation else ""
        ax[0].set_title(f"Preview of {Path(fits_file).stem}{observation_id_text}")
        fig.savefig(fname=f"{Path(fits_file).with_suffix('.png')}", bbox_inches="tight")
    plt.close(fig)

    if not interactive_mode:
        pylab.ion()  # Turn interactive mode back on
