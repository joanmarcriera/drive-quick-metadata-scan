"""Duplicate folder detection based on deterministic folder hashes."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from gdrive_dedupe.storage.database import Database


@dataclass(slots=True)
class FolderRecord:
    id: str
    name: str
    parent: str | None


@dataclass(slots=True)
class DuplicateFolderGroup:
    hash_value: str
    count: int
    folders: list[FolderRecord]


@dataclass(slots=True)
class ActionableDuplicateRootGroup:
    hash_value: str
    count: int
    folders: list[FolderRecord]


@dataclass(slots=True)
class FolderSubtreeStats:
    folder_count: int
    file_count: int
    total_size: int


@dataclass(slots=True)
class ActionableRootRecommendation:
    hash_value: str
    copies: int
    keep: FolderRecord
    delete_candidates: list[FolderRecord]
    subtree: FolderSubtreeStats
    estimated_reclaimable_bytes: int


def get_duplicate_folder_groups(
    database: Database, limit: int | None = None
) -> list[DuplicateFolderGroup]:
    sql = (
        "SELECT hash, COUNT(*) AS cnt "
        "FROM folder_hash "
        "GROUP BY hash "
        "HAVING COUNT(*) > 1 "
        "ORDER BY cnt DESC"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    groups = []
    for row in database.execute(sql).fetchall():
        hash_value = str(row["hash"])
        folder_rows = database.execute(
            """
            SELECT f.id, f.name, f.parent
            FROM folder_hash AS fh
            JOIN folders AS f ON f.id = fh.folder_id
            WHERE fh.hash = ?
            ORDER BY f.parent, f.name
            """,
            (hash_value,),
        ).fetchall()
        folders = [
            FolderRecord(id=str(f["id"]), name=str(f["name"]), parent=f["parent"])
            for f in folder_rows
        ]
        groups.append(
            DuplicateFolderGroup(hash_value=hash_value, count=int(row["cnt"]), folders=folders)
        )

    return groups


def get_actionable_duplicate_root_groups(
    database: Database, limit: int | None = None
) -> list[ActionableDuplicateRootGroup]:
    rows = database.execute("""
        SELECT fh.hash AS hash_value, f.id, f.name, f.parent
        FROM folder_hash AS fh
        JOIN folders AS f ON f.id = fh.folder_id
        JOIN (
            SELECT hash
            FROM folder_hash
            GROUP BY hash
            HAVING COUNT(*) > 1
        ) AS dup ON dup.hash = fh.hash
        ORDER BY fh.hash, f.parent, f.name
        """).fetchall()

    if not rows:
        return []

    members_by_hash: dict[str, list[FolderRecord]] = {}
    duplicate_ids: set[str] = set()
    for row in rows:
        folder = FolderRecord(
            id=str(row["id"]),
            name=str(row["name"]),
            parent=row["parent"],
        )
        hash_value = str(row["hash_value"])
        members_by_hash.setdefault(hash_value, []).append(folder)
        duplicate_ids.add(folder.id)

    actionable_groups: list[ActionableDuplicateRootGroup] = []
    for hash_value, members in members_by_hash.items():
        # Collapse nested duplicate noise: keep only folders whose parent is not
        # itself part of any duplicate folder group.
        top_level_members = [folder for folder in members if folder.parent not in duplicate_ids]
        if len(top_level_members) < 2:
            continue

        actionable_groups.append(
            ActionableDuplicateRootGroup(
                hash_value=hash_value,
                count=len(top_level_members),
                folders=top_level_members,
            )
        )

    actionable_groups.sort(key=lambda group: group.count, reverse=True)
    if limit is not None:
        return actionable_groups[: int(limit)]
    return actionable_groups


def get_folder_subtree_stats(database: Database, folder_id: str) -> FolderSubtreeStats:
    row = database.execute(
        """
        WITH RECURSIVE subtree(id) AS (
            SELECT ?
            UNION
            SELECT f.id
            FROM folders AS f
            JOIN subtree AS s ON f.parent = s.id
        )
        SELECT
            (SELECT COUNT(*) FROM subtree) AS folder_count,
            (SELECT COUNT(*) FROM files WHERE parent IN (SELECT id FROM subtree)) AS file_count,
            (
                SELECT COALESCE(SUM(size), 0)
                FROM files
                WHERE size IS NOT NULL AND parent IN (SELECT id FROM subtree)
            ) AS total_size
        """,
        (folder_id,),
    ).fetchone()

    if row is None:
        return FolderSubtreeStats(folder_count=0, file_count=0, total_size=0)

    return FolderSubtreeStats(
        folder_count=int(row["folder_count"]),
        file_count=int(row["file_count"]),
        total_size=int(row["total_size"]),
    )


def compute_all_folder_subtree_stats(database: Database) -> dict[str, FolderSubtreeStats]:
    folder_rows = database.execute("SELECT id, parent FROM folders").fetchall()
    if not folder_rows:
        return {}

    parent_by_id: dict[str, str | None] = {}
    pending_children: dict[str, int] = {}
    total_folder_count: dict[str, int] = {}
    total_file_count: dict[str, int] = {}
    total_size: dict[str, int] = {}

    for row in folder_rows:
        folder_id = str(row["id"])
        parent = str(row["parent"]) if row["parent"] is not None else None
        parent_by_id[folder_id] = parent
        pending_children[folder_id] = 0
        total_folder_count[folder_id] = 1
        total_file_count[folder_id] = 0
        total_size[folder_id] = 0

    for _folder_id, parent in parent_by_id.items():
        if parent is not None and parent in pending_children:
            pending_children[parent] += 1

    file_rows = database.execute("""
        SELECT parent, COUNT(*) AS file_count, COALESCE(SUM(size), 0) AS total_size
        FROM files
        WHERE parent IS NOT NULL
        GROUP BY parent
        """).fetchall()
    for row in file_rows:
        parent = str(row["parent"])
        if parent not in total_file_count:
            continue
        total_file_count[parent] = int(row["file_count"])
        total_size[parent] = int(row["total_size"])

    queue: deque[str] = deque(
        folder_id for folder_id, children in pending_children.items() if children == 0
    )
    while queue:
        folder_id = queue.popleft()
        parent = parent_by_id.get(folder_id)
        if parent is None or parent not in pending_children:
            continue

        total_folder_count[parent] += total_folder_count[folder_id]
        total_file_count[parent] += total_file_count[folder_id]
        total_size[parent] += total_size[folder_id]
        pending_children[parent] -= 1
        if pending_children[parent] == 0:
            queue.append(parent)

    return {
        folder_id: FolderSubtreeStats(
            folder_count=total_folder_count[folder_id],
            file_count=total_file_count[folder_id],
            total_size=total_size[folder_id],
        )
        for folder_id in parent_by_id
    }


def get_actionable_root_recommendations(
    database: Database,
    *,
    limit: int | None = None,
    offset: int = 0,
    min_reclaimable_bytes: int = 1,
) -> list[ActionableRootRecommendation]:
    root_groups = get_actionable_duplicate_root_groups(database, limit=None)
    if not root_groups:
        return []

    subtree_stats_by_folder = compute_all_folder_subtree_stats(database)
    parent_by_id = {
        str(row["id"]): (str(row["parent"]) if row["parent"] is not None else None)
        for row in database.execute("SELECT id, parent FROM folders").fetchall()
    }

    recommendations: list[ActionableRootRecommendation] = []
    for group in root_groups:
        keep = choose_keep_candidate(group.folders, parent_by_id=parent_by_id)
        subtree = subtree_stats_by_folder.get(
            keep.id,
            FolderSubtreeStats(folder_count=0, file_count=0, total_size=0),
        )
        estimated_reclaimable = max(group.count - 1, 0) * subtree.total_size
        if estimated_reclaimable < min_reclaimable_bytes:
            continue

        recommendations.append(
            ActionableRootRecommendation(
                hash_value=group.hash_value,
                copies=group.count,
                keep=keep,
                delete_candidates=[f for f in group.folders if f.id != keep.id],
                subtree=subtree,
                estimated_reclaimable_bytes=estimated_reclaimable,
            )
        )

    recommendations.sort(
        key=lambda item: (
            item.estimated_reclaimable_bytes,
            item.copies,
            item.subtree.file_count,
        ),
        reverse=True,
    )

    start = max(offset, 0)
    if limit is None:
        return recommendations[start:]
    return recommendations[start : start + max(limit, 0)]


def choose_keep_candidate(
    folders: list[FolderRecord],
    *,
    parent_by_id: dict[str, str | None],
) -> FolderRecord:
    depth_cache: dict[str, int] = {}

    def folder_depth(folder_id: str) -> int:
        if folder_id in depth_cache:
            return depth_cache[folder_id]
        parent = parent_by_id.get(folder_id)
        if parent is None or parent not in parent_by_id:
            depth_cache[folder_id] = 1
            return 1
        depth_cache[folder_id] = 1 + folder_depth(parent)
        return depth_cache[folder_id]

    ranked = sorted(
        folders,
        key=lambda folder: (
            folder_depth(folder.id),
            len(folder.name),
            folder.name.lower(),
            folder.id,
        ),
    )
    return ranked[0]
