"""Assemble the five-slide comparison PPTX."""

from __future__ import annotations

from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Emu, Inches, Pt

from bedrock.analysis.electricity.current.diagnostics.deck.data import (
    ImplBundle,
    fill_mixed_c_col,
)
from bedrock.analysis.electricity.current.diagnostics.deck.histograms import (
    write_hist_png,
)
from bedrock.analysis.electricity.current.diagnostics.deck.pairs import Pair
from bedrock.analysis.electricity.current.diagnostics.deck.tables import (
    TableGrid,
    class_mwh_grids,
    ef_grids,
)

SLIDE_W = Inches(13.333)
SLIDE_H = Inches(7.5)
HEADER_BG = RGBColor(0x1F, 0x4E, 0x79)
HEADER_FG = RGBColor(0xFF, 0xFF, 0xFF)
ALT_ROW = RGBColor(0xD6, 0xE3, 0xF0)
SAME_FG = RGBColor(0x2E, 0x7D, 0x32)
NAVY = RGBColor(0x1F, 0x4E, 0x79)


def _set_run_font(
    run, *, size_pt: float, bold: bool = False, color: RGBColor | None = None
) -> None:
    run.font.size = Pt(size_pt)
    run.font.bold = bold
    run.font.name = 'Calibri'
    if color is not None:
        run.font.color.rgb = color


def _add_title(slide, text: str) -> None:
    box = slide.shapes.add_textbox(
        Inches(0.35), Inches(0.12), Inches(12.6), Inches(0.45)
    )
    tf = box.text_frame
    tf.clear()
    p = tf.paragraphs[0]
    run = p.add_run()
    run.text = text
    _set_run_font(run, size_pt=18, bold=True, color=NAVY)


def _add_notes(slide, text: str, top_in: float = 6.55) -> None:
    box = slide.shapes.add_textbox(
        Inches(0.35), Inches(top_in), Inches(12.6), Inches(0.8)
    )
    tf = box.text_frame
    tf.word_wrap = True
    tf.clear()
    p = tf.paragraphs[0]
    run = p.add_run()
    run.text = text
    _set_run_font(run, size_pt=10, color=RGBColor(0x33, 0x33, 0x33))


def _fill_cell(cell, text: str, *, header: bool, alt: bool) -> None:
    cell.text = ''
    p = cell.text_frame.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    run = p.add_run()
    run.text = text
    if header:
        _set_run_font(run, size_pt=9, bold=True, color=HEADER_FG)
        cell.fill.solid()
        cell.fill.fore_color.rgb = HEADER_BG
    else:
        color = SAME_FG if text == 'same' else RGBColor(0x22, 0x22, 0x22)
        _set_run_font(run, size_pt=9, bold=text == 'same', color=color)
        if alt:
            cell.fill.solid()
            cell.fill.fore_color.rgb = ALT_ROW
    cell.margin_left = Emu(40000)
    cell.margin_right = Emu(40000)
    cell.margin_top = Emu(20000)
    cell.margin_bottom = Emu(20000)
    # Keep cell text from overflowing
    cell.text_frame.word_wrap = True


def _add_table(
    slide,
    grid: TableGrid,
    *,
    left: float,
    top: float,
    width: float,
    height: float,
) -> None:
    caption = slide.shapes.add_textbox(
        Inches(left), Inches(top - 0.32), Inches(width), Inches(0.3)
    )
    tf = caption.text_frame
    tf.clear()
    p = tf.paragraphs[0]
    run = p.add_run()
    run.text = grid.title
    _set_run_font(run, size_pt=12, bold=True, color=NAVY)

    n_rows = 1 + len(grid.rows)
    n_cols = len(grid.headers)
    table = slide.shapes.add_table(
        n_rows, n_cols, Inches(left), Inches(top), Inches(width), Inches(height)
    ).table
    for j, header in enumerate(grid.headers):
        table.columns[j].width = Inches(width / n_cols)
        _fill_cell(table.cell(0, j), header, header=True, alt=False)
    for i, row in enumerate(grid.rows):
        for j, value in enumerate(row):
            _fill_cell(table.cell(i + 1, j), value, header=False, alt=i % 2 == 1)


def _two_tables_slide(
    prs: Presentation,
    title: str,
    note: str,
    left_grid: TableGrid,
    right_grid: TableGrid,
) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _add_title(slide, title)
    _add_table(slide, left_grid, left=0.3, top=1.0, width=6.2, height=4.8)
    _add_table(slide, right_grid, left=6.8, top=1.0, width=6.2, height=4.8)
    _add_notes(slide, note)


def _image_slide(prs: Presentation, title: str, caption: str, png: Path) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _add_title(slide, title)
    slide.shapes.add_picture(
        str(png), Inches(0.25), Inches(0.6), width=Inches(12.8), height=Inches(6.0)
    )
    _add_notes(slide, caption, top_in=6.65)


def write_pptx(
    pair: Pair,
    top: ImplBundle,
    bottom: ImplBundle,
    out_path: Path,
    *,
    png_dir: Path,
) -> Path:
    fill_mixed_c_col(top)
    fill_mixed_c_col(bottom)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    png_dir.mkdir(parents=True, exist_ok=True)
    d_png = write_hist_png(pair, top, bottom, 'D', png_dir / f'{pair.key}_D.png')
    n_png = write_hist_png(pair, top, bottom, 'N', png_dir / f'{pair.key}_N.png')

    class_top, class_bottom = class_mwh_grids(pair, top, bottom)
    d_top, d_bottom = ef_grids(pair, top, bottom, 'D')
    n_top, n_bottom = ef_grids(pair, top, bottom, 'N')

    prs = Presentation()
    prs.slide_width = SLIDE_W
    prs.slide_height = SLIDE_H

    _two_tables_slide(
        prs,
        'Overview of results: Electricity MWh by use class',
        pair.slide1_note,
        class_top,
        class_bottom,
    )
    _two_tables_slide(
        prs,
        'Overview of results: D for electricity sectors',
        pair.slide_ef_note
        + ' Electricity D EFs by disaggregation step (kg CO₂e / USD).',
        d_top,
        d_bottom,
    )
    _image_slide(
        prs,
        'Overview of results: D for electricity sectors',
        pair.slide3_caption,
        d_png,
    )
    slide4_note = pair.slide_ef_note
    if pair.slide4_extra_note:
        slide4_note = f'{slide4_note} {pair.slide4_extra_note}'
    _two_tables_slide(
        prs,
        'Overview of results: N for electricity sectors',
        slide4_note + ' Electricity N EFs by disaggregation step (kg CO₂e / USD).',
        n_top,
        n_bottom,
    )
    _image_slide(
        prs,
        'Overview of results: N for electricity sectors',
        pair.slide5_caption,
        n_png,
    )
    prs.save(str(out_path))
    return out_path
