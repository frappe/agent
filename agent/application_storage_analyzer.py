from __future__ import annotations

import json


def format_size(bytes_val):
    thresholds = [(1024**3, "G"), (1024**2, "M"), (1024, "K")]

    for factor, suffix in thresholds:
        if bytes_val >= factor:
            value = bytes_val / factor
            return f"{value:.2f}{suffix}"

    return f"{bytes_val}B"


def calculate_directory_size(entry: list) -> int:
    """
    Recursively calculate the total size of a directory entry.

    ncdu JSON structure:
    - File: {"name": "file.txt", "asize": 1024}
    - Directory: [{"name": "dir", "dsize": 4096}, child1, child2, ...]

    We need to sum up all file sizes (asize) in the tree.
    """
    if not isinstance(entry, list) or len(entry) == 0:
        return 0

    metadata = entry[0]
    if not isinstance(metadata, dict):
        return 0

    total_size = 0

    # If this is a file (has asize but no dsize), return its size
    if "asize" in metadata and "dsize" not in metadata:
        return metadata["asize"]

    # If this is a directory, recursively sum all children
    if len(entry) > 1:
        for child in entry[1:]:
            if isinstance(child, list):
                total_size += calculate_directory_size(child)
            elif isinstance(child, dict) and "asize" in child:
                # Direct file entry
                total_size += child["asize"]

    return total_size


def build_tree_structure(  # noqa: C901
    entry: list, display_depth: int = 10, current_depth: int = 0, max_children: int = 5
) -> dict:
    """Build tree structure with calculated sizes - calculate all the way dow but we display limited depth"""
    if not isinstance(entry, list) or len(entry) == 0:
        return None

    metadata = entry[0]
    if not isinstance(metadata, dict) or "name" not in metadata:
        return None

    total_size = calculate_directory_size(entry)

    node = {
        "name": metadata["name"],
        "size": total_size,
        "size_formatted": format_size(total_size),
        "is_file": "asize" in metadata and "dsize" not in metadata,
        "children": [],
    }

    # Only show children if we haven't reached display depth limit
    if current_depth < display_depth and len(entry) > 1:
        children_with_sizes = []

        for child in entry[1:]:
            if isinstance(child, list):
                # Calculate size for this child (this calculation goes all the way down)
                child_size = calculate_directory_size(child)
                children_with_sizes.append((child_size, child))
            elif isinstance(child, dict) and "name" in child:
                # Direct file entry - convert to list format
                child_size = child.get("asize", 0)
                children_with_sizes.append((child_size, [child]))

        # Sort by size (descending) and take top 5
        children_with_sizes.sort(key=lambda x: x[0], reverse=True)

        for _, child_entry in children_with_sizes[:max_children]:
            child_node = build_tree_structure(child_entry, display_depth, current_depth + 1, max_children)
            if child_node and child_node["size"] > 1:
                node["children"].append(child_node)

    return node


def analyze_ncdu_output(json_data: str, display_depth: int = 5, max_children: int = 5) -> dict | None:
    data = json.loads(json_data)

    if isinstance(data, list) and len(data) >= 4:
        # Standard ncdu format: [version, timestamp, metadata, root_tree]
        root_entry = data[3]

        if isinstance(root_entry, list) and len(root_entry) > 0:
            return build_tree_structure(root_entry, display_depth, 0, max_children)

    return None
