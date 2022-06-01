from typing import Tuple
from functools import lru_cache
import time

from runtype import dataclass

from rich.console import RenderableType
import rich.repr
from rich.text import Text

from textual.message import Message
from textual.reactive import Reactive
from textual._types import MessageTarget
from textual.widgets import TreeControl, TreeClick, TreeNode, NodeID
from textual.app import App


@dataclass(frozen=False)
class SegmentInfo:
    is_diff: bool = None
    diff_count: int = None
    start_key: int = None
    end_key: int = None
    counts: Tuple[int, ...] = None
    diff: list = None

    def get_label(self):
        label = ""
        if self.start_key and self.end_key:
            label += f"{self.start_key}..{self.end_key} | size "
            if self.counts:
                label += f"= {self.counts[0]}"
            else:
                label += f"<= {self.end_key - self.start_key}"

        if self.counts and self.diff_count:
            if self.counts[0]:
                label += f" | {100 * self.diff_count / self.counts[0]:.2f}% diff"

        return label

    def get_icon(self):
        if self.is_diff is None:
            return "❓"

        return "❌" if self.is_diff else "✔️"

    def __hash__(self):
        return hash((self.is_diff, self.diff_count, self.start_key, self.end_key, self.counts))


@rich.repr.auto
class SegmentClick(Message, bubble=True):
    def __init__(self, sender: MessageTarget, node: str) -> None:
        self.node = node
        super().__init__(sender)


GUIDE_STYLE = "gray23"
GUIDE_STYLE_HOVER = "bold not dim gray50"


class SegmentTree(TreeControl[SegmentInfo]):
    def __init__(self, label) -> None:
        data = SegmentInfo()
        super().__init__(label, name="root", data=data)
        self.root.tree.guide_style = GUIDE_STYLE

    has_focus: Reactive[bool] = Reactive(False)

    def on_focus(self) -> None:
        self.has_focus = True

    def on_blur(self) -> None:
        self.has_focus = False

    async def watch_hover_node(self, hover_node: NodeID) -> None:
        for node in self.nodes.values():
            node.tree.guide_style = GUIDE_STYLE_HOVER if node.id == hover_node else GUIDE_STYLE
        self.refresh(layout=True)

    def render_node(self, node: TreeNode[SegmentInfo]) -> RenderableType:
        return self.render_tree_label(
            node,
            node.data,
            node.expanded,
            node.is_cursor,
            node.id == self.hover_node,
            self.has_focus,
        )

    @lru_cache(maxsize=1024 * 32)
    def render_tree_label(
        self,
        node: TreeNode[SegmentInfo],
        data,
        expanded: bool,
        is_cursor: bool,
        is_hover: bool,
        has_focus: bool,
    ) -> RenderableType:
        meta = {
            "@click": f"click_label({node.id})",
            "tree_node": node.id,
            "cursor": node.is_cursor,
        }
        label = data.get_label()  # node.label
        # if node.children:
        #     p = 100 * len([1 for c in node.children if c.data.is_diff is not None]) // len(node.children)
        #     label += f" [{p}%]"

        label = Text(label) if isinstance(label, str) else label
        if is_hover:
            label.stylize("underline")

        # if len(node.children) > 0:
        #     icon = "📂" if expanded else "📁"

        label.stylize("white")

        label.highlight_regex(r"\d*", "cyan")

        if label.plain.startswith("."):
            label.stylize("dim")

        if is_cursor and has_focus:
            label.stylize("reverse")

        icon_label = Text(f"{data.get_icon()} ", no_wrap=True, overflow="ellipsis") + label
        icon_label.apply_meta(meta)
        return icon_label

    async def handle_tree_click(self, message: TreeClick[SegmentInfo]) -> None:
        await message.node.toggle()

        await self.emit(SegmentClick(self, message.node))


import os
import sys
import time
import asyncio
from threading import Thread

from rich.text import Text
import traceback

from textual.app import App
from textual.widgets import Header, Footer, ScrollView

from data_diff import diff_tables, TableRef, TableDiffer


class StatsTree:
    def __init__(self, node, *tables):
        assert tables
        self.tables = tables
        self.node = node

    def update_tables(self, *tables):
        self.tables = tables

    def set_diff(self, diff):
        si = self.node.data
        if diff:
            si.is_diff = True
            si.diff_count = len(diff)
            si.diff = diff
        else:
            si.is_diff = False

    def set_count_and_checksum(self, counts, checksums):
        si = self.node.data
        si.counts = tuple(counts)

        s1, s2 = checksums
        si.is_diff = s1 != s2

    def add(self, table1, table2):
        si = SegmentInfo(
            start_key=table1.start_key,
            end_key=table1.end_key,
        )
        asyncio.run(self.node.add("-", si))
        asyncio.run(self.node.expand())
        return StatsTree(self.node.children[-1], table1, table2)


def run_diff_thread(differ, tables, tree, log):
    try:
        start = time.time()
        log.append("Starting!")

        stats_tree = StatsTree(tree.root, *tables)

        diff = []
        for t in differ.diff_tables(*tables, stats_tree=stats_tree):
            diff += t
            stats_tree.set_diff(diff)

        stats_tree.set_diff(diff)

        end = time.time() - start
        log.append(f"Done in {end:.2f} seconds.")
    except Exception as e:
        log.append(f"{type(e)} - {traceback.format_exc()}")


class FeedViewer(ScrollView):
    def __init__(self) -> None:
        self.lines = []

        super().__init__()

    def append(self, line):
        self.lines.append(line)
        # asyncio.run( self.update_view() )

    async def append_async(self, line):
        self.lines.append(line)
        # await self.update_view()

    async def update_view(self):
        await self.update(Text("\n".join(str(x) for x in self.lines)), home=False)

    async def on_mount(self):
        self.set_interval(0.1, self.update_view)


class DataDiffApp(App):
    """An example of a very simple Textual App"""

    def __init__(self, differ, tables, **kw) -> None:
        self.differ = differ
        self.tables = tables
        super().__init__(**kw)

    async def on_load(self) -> None:
        """Sent before going in to application mode."""

        # Bind our basic keys
        await self.bind("b", "view.toggle('sidebar')", "Toggle tree")
        await self.bind("q", "quit", "Quit")

    async def on_mount(self) -> None:
        """Call after terminal goes in to application mode"""

        # Create our widgets
        # In this a scroll view for the code and a directory tree
        self.logview = FeedViewer()
        self.diffview = ScrollView()
        self.tree = SegmentTree("-")

        await self.diffview.update(Text("~"))

        # Dock our widgets
        await self.view.dock(Header(), edge="top")
        await self.view.dock(Footer(), edge="bottom")

        # Note the directory is also in a scroll view
        await self.view.dock(ScrollView(self.tree), edge="left", size=80, name="sidebar")
        await self.view.dock(self.logview, self.diffview, edge="top")

        t = Thread(target=run_diff_thread, args=[self.differ, self.tables, self.tree, self.logview])
        t.daemon = True
        t.start()

    async def handle_segment_click(self, message: SegmentClick) -> None:
        """A message sent by the directory tree when a file is clicked."""

        si: SegmentInfo = message.node.data

        lines = [
            f"Diff for segment {si.start_key}..{si.end_key}",
            "----------------------------------------------",
            "",
        ]

        if si.diff:
            lines += map(str, si.diff)
        else:
            if si.is_diff is None:
                lines.append("Unknown; processing...")
            elif si.is_diff:
                lines.append("Different!")
            else:
                lines.append("0 different rows.")

        text = "\n".join(lines)
        await self.diffview.update(Text(text))


def start_app(differ, *tables):
    DataDiffApp.run(title="Data-diff", log="textual.log", differ=differ, tables=tables)


def test():
    table1 = TableRef("postgres:///", "Rating")
    table2 = TableRef("postgres:///", "Rating_del1")

    tables = [table1, table2]
    segments = [t.create_table_segment(key_column="id", update_column="timestamp", thread_count=4) for t in tables]

    differ = TableDiffer(
        bisection_factor=32,
        max_threadpool_size=8,
    )

    start_app(differ, *segments)


if __name__ == "__main__":
    test()
