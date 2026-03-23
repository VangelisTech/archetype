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

import json
import os
from pathlib import Path
from typing import Dict, Generator, List, Optional, Set, Tuple

from .components import Dedup, ProjectContext


class ConversationScanner:
    """Scans all .claude/projects/* directories for conversation JSONLs."""
    
    def __init__(self, base_path: Optional[Path] = None):
        """Initialize scanner with optional base path. Defaults to user's home directory."""
        if base_path is None:
            base_path = Path.home()
        self.base_path = base_path
        self.processed_files: Set[str] = set()
        self.dedup_store: Set[Dedup] = set()
    
    def find_claude_projects(self) -> List[Path]:
        """Find all .claude/projects/* directories."""
        claude_base = self.base_path / ".claude" / "projects"
        if not claude_base.exists():
            return []
        
        projects = []
        for item in claude_base.iterdir():
            if item.is_dir():
                projects.append(item)
        
        return projects
    
    def scan_project_conversations(self, project_path: Path) -> Generator[Tuple[ProjectContext, Dict], None, None]:
        """Scan a single project directory for conversation JSONLs."""
        project_context = ProjectContext.from_project_path(project_path)
        
        # Look for JSON files in the project directory
        for json_file in project_path.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    conversation_data = json.load(f)
                
                # Store file path for incremental processing
                file_key = str(json_file.absolute())
                if file_key not in self.processed_files:
                    yield project_context, conversation_data
                    self.processed_files.add(file_key)
                    
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not read {json_file}: {e}")
                continue
    
    def scan_all_projects(self) -> Generator[Tuple[ProjectContext, Dict], None, None]:
        """Scan all .claude projects for conversations."""
        projects = self.find_claude_projects()
        
        for project_path in projects:
            yield from self.scan_project_conversations(project_path)
    
    def is_duplicate_content(self, content: str) -> bool:
        """Check if content is duplicate using deduplication store."""
        dedup_component = Dedup.from_content(content)
        if dedup_component in self.dedup_store:
            return True
        
        self.dedup_store.add(dedup_component)
        return False
    
    def get_new_conversations_since(self, last_run_file: Optional[Path] = None) -> Generator[Tuple[ProjectContext, Dict], None, None]:
        """Get only new conversations since the last run."""
        if last_run_file and last_run_file.exists():
            try:
                with open(last_run_file, 'r') as f:
                    processed_data = json.load(f)
                    self.processed_files = set(processed_data.get('processed_files', []))
            except (json.JSONDecodeError, IOError):
                print(f"Warning: Could not read last run file {last_run_file}")
        
        yield from self.scan_all_projects()
        
        # Save processed files for next incremental run
        if last_run_file:
            try:
                last_run_file.parent.mkdir(parents=True, exist_ok=True)
                with open(last_run_file, 'w') as f:
                    json.dump({
                        'processed_files': list(self.processed_files),
                        'total_processed': len(self.processed_files)
                    }, f, indent=2)
            except IOError as e:
                print(f"Warning: Could not save last run file: {e}")