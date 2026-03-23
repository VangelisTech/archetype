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
from typing import Dict, List, Optional

import lancedb

from .components import Memory


class MemoryStorage:
    """Handles persistence of memories to LanceDB."""
    
    def __init__(self, storage_path: Optional[Path] = None):
        """Initialize storage with optional custom path."""
        if storage_path is None:
            storage_path = Path.home() / ".archetype" / "mind_extraction"
        
        self.storage_path = storage_path
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize LanceDB connection
        self.db = lancedb.connect(str(self.storage_path / "memories.lance"))
        self.table_name = "memories"
        
        # Define expected fields for memories (schema will be inferred)
        self.expected_fields = [
            "id", "content", "source_project", "conversation_file",
            "timestamp", "confidence_score", "memory_type", "content_hash"
        ]
    
    def _ensure_table_exists(self):
        """Ensure the memories table exists."""
        try:
            # Try to open existing table
            self.table = self.db.open_table(self.table_name)
        except FileNotFoundError:
            # Create new table if it doesn't exist
            self.table = None  # Will be created when first memories are added
    
    def save_memories(self, memories: List[Memory]) -> int:
        """Save memories to LanceDB and return count of new memories saved."""
        if not memories:
            return 0
        
        # Prepare data
        new_memories = []
        existing_hashes = set()
        
        # Get existing content hashes to avoid duplicates
        if self.table:
            try:
                existing_data = self.table.to_pandas()
                if len(existing_data) > 0:
                    existing_hashes = set(existing_data['content_hash'].tolist())
            except Exception:
                # Table might be empty or not exist
                pass
        
        # Filter out duplicates and prepare data
        for i, memory in enumerate(memories):
            content_hash = self._generate_memory_id(memory)
            if content_hash not in existing_hashes:
                memory_dict = {
                    "id": f"{memory.source_project}_{content_hash[:8]}_{i}",
                    "content": memory.content,
                    "source_project": memory.source_project,
                    "conversation_file": memory.conversation_file,
                    "timestamp": memory.timestamp,
                    "confidence_score": memory.confidence_score,
                    "memory_type": memory.memory_type,
                    "content_hash": content_hash
                }
                new_memories.append(memory_dict)
                existing_hashes.add(content_hash)
        
        if new_memories:
            if self.table is None:
                # Create table with first batch of memories
                self.table = self.db.create_table(self.table_name, new_memories)
            else:
                # Append to existing table
                self.table.add(new_memories)
        
        return len(new_memories)
    
    def _generate_memory_id(self, memory: Memory) -> str:
        """Generate a unique ID for a memory based on content hash."""
        import hashlib
        content = f"{memory.content}_{memory.source_project}_{memory.timestamp}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def get_all_memories(self) -> List[Dict]:
        """Retrieve all memories from storage."""
        try:
            self._ensure_table_exists()
            if self.table is None:
                return []
            pandas_data = self.table.to_pandas()
            return pandas_data.to_dict('records')
        except Exception as e:
            print(f"Error retrieving memories: {e}")
            return []
    
    def get_memories_by_project(self, project_name: str) -> List[Dict]:
        """Retrieve memories for a specific project."""
        try:
            self._ensure_table_exists()
            if self.table is None:
                return []
            results = self.table.search().where(f"source_project = '{project_name}'").to_pandas()
            return results.to_dict('records')
        except Exception as e:
            print(f"Error retrieving memories for project {project_name}: {e}")
            return []
    
    def get_memory_count(self) -> int:
        """Get total count of stored memories."""
        try:
            self._ensure_table_exists()
            if self.table is None:
                return 0
            return len(self.table)
        except Exception as e:
            print(f"Error counting memories: {e}")
            return 0
    
    def search_memories(self, query: str, limit: int = 10) -> List[Dict]:
        """Search memories by content (if full-text search is available)."""
        try:
            self._ensure_table_exists()
            # Simple content filter for now
            all_memories = self.get_all_memories()
            results = []
            query_lower = query.lower()
            
            for memory in all_memories:
                if query_lower in memory['content'].lower():
                    results.append(memory)
                    if len(results) >= limit:
                        break
            
            return results
        except Exception as e:
            print(f"Error searching memories: {e}")
            return []
    
    def export_profile_to_json(self, profile: Dict, output_path: Optional[Path] = None) -> Path:
        """Export aggregated profile to JSON file."""
        if output_path is None:
            output_path = self.storage_path / "unified_mind_profile.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(profile, f, indent=2, ensure_ascii=False)
        
        return output_path