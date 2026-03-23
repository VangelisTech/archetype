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

from collections import defaultdict
from typing import Dict, List

from .components import Memory


class MemoryAggregator:
    """Aggregates memories across projects into a unified theory-of-mind profile."""
    
    def __init__(self):
        self.memories_by_project: Dict[str, List[Memory]] = defaultdict(list)
        self.memories_by_type: Dict[str, List[Memory]] = defaultdict(list)
        self.all_memories: List[Memory] = []
    
    def add_memories(self, memories: List[Memory]):
        """Add memories to the aggregator."""
        for memory in memories:
            self.all_memories.append(memory)
            self.memories_by_project[memory.source_project].append(memory)
            self.memories_by_type[memory.memory_type].append(memory)
    
    def get_unified_profile(self) -> Dict:
        """Generate a unified theory-of-mind profile."""
        profile = {
            "total_memories": len(self.all_memories),
            "projects_covered": list(self.memories_by_project.keys()),
            "memory_types": {
                memory_type: len(memories) 
                for memory_type, memories in self.memories_by_type.items()
            },
            "project_breakdown": {
                project: len(memories) 
                for project, memories in self.memories_by_project.items()
            },
            "high_confidence_memories": [
                {
                    "content": memory.content[:200] + "..." if len(memory.content) > 200 else memory.content,
                    "project": memory.source_project,
                    "type": memory.memory_type,
                    "confidence": memory.confidence_score
                }
                for memory in self.all_memories 
                if memory.confidence_score >= 0.8
            ],
            "technical_insights": self._extract_technical_insights(),
            "behavioral_patterns": self._extract_behavioral_patterns(),
            "preferences": self._extract_preferences()
        }
        
        return profile
    
    def _extract_technical_insights(self) -> List[Dict]:
        """Extract technical insights from technical memories."""
        technical_memories = self.memories_by_type.get("technical", [])
        
        insights = []
        for memory in technical_memories:
            if memory.confidence_score >= 0.7:
                insights.append({
                    "insight": memory.content[:150] + "..." if len(memory.content) > 150 else memory.content,
                    "project": memory.source_project,
                    "confidence": memory.confidence_score
                })
        
        return insights[:10]  # Return top 10
    
    def _extract_behavioral_patterns(self) -> List[Dict]:
        """Extract behavioral patterns from behavioral memories."""
        behavioral_memories = self.memories_by_type.get("behavioral", [])
        
        patterns = []
        for memory in behavioral_memories:
            if memory.confidence_score >= 0.7:
                patterns.append({
                    "pattern": memory.content[:150] + "..." if len(memory.content) > 150 else memory.content,
                    "project": memory.source_project,
                    "confidence": memory.confidence_score
                })
        
        return patterns[:10]  # Return top 10
    
    def _extract_preferences(self) -> List[Dict]:
        """Extract preferences from preference memories."""
        preference_memories = self.memories_by_type.get("preference", [])
        
        preferences = []
        for memory in preference_memories:
            if memory.confidence_score >= 0.7:
                preferences.append({
                    "preference": memory.content[:150] + "..." if len(memory.content) > 150 else memory.content,
                    "project": memory.source_project,
                    "confidence": memory.confidence_score
                })
        
        return preferences[:10]  # Return top 10
    
    def get_project_specific_profile(self, project_name: str) -> Dict:
        """Get a profile specific to one project."""
        project_memories = self.memories_by_project.get(project_name, [])
        
        if not project_memories:
            return {"error": f"No memories found for project: {project_name}"}
        
        # Group by type for this project
        project_types = defaultdict(list)
        for memory in project_memories:
            project_types[memory.memory_type].append(memory)
        
        return {
            "project": project_name,
            "total_memories": len(project_memories),
            "memory_types": {
                memory_type: len(memories) 
                for memory_type, memories in project_types.items()
            },
            "memories": [
                {
                    "content": memory.content[:200] + "..." if len(memory.content) > 200 else memory.content,
                    "type": memory.memory_type,
                    "confidence": memory.confidence_score,
                    "timestamp": memory.timestamp
                }
                for memory in sorted(project_memories, key=lambda m: m.confidence_score, reverse=True)
            ][:20]  # Top 20 memories
        }