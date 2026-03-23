# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class ProjectContext:
    """Component that tracks which project/repo a conversation is about."""
    project_name: str
    project_path: str
    repo_url: Optional[str] = None
    
    @classmethod
    def from_project_path(cls, project_path: Path) -> "ProjectContext":
        """Extract project context from a .claude/projects/* directory path."""
        project_name = project_path.name
        return cls(
            project_name=project_name,
            project_path=str(project_path),
            repo_url=None  # Could be extracted from git config if needed
        )


@dataclass
class Dedup:
    """Component for content deduplication using content hashing."""
    content_hash: str
    message_content: str
    
    @classmethod
    def from_content(cls, content: str) -> "Dedup":
        """Create deduplication component from message content."""
        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        return cls(
            content_hash=content_hash,
            message_content=content
        )
    
    def __eq__(self, other) -> bool:
        """Check if two Dedup components are equal based on content hash."""
        if not isinstance(other, Dedup):
            return False
        return self.content_hash == other.content_hash
    
    def __hash__(self) -> int:
        """Hash based on content hash for set operations."""
        return hash(self.content_hash)


@dataclass
class Memory:
    """Extracted memory component from conversation analysis."""
    content: str
    source_project: str
    conversation_file: str
    timestamp: str
    confidence_score: float = 0.0
    memory_type: str = "general"  # general, technical, behavioral, etc.