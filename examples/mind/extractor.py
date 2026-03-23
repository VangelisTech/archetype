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

from typing import Dict, List, Optional

from .components import Memory, ProjectContext


class MemoryExtractor:
    """Extracts memories from conversation data."""
    
    def __init__(self, llm_client=None):
        """Initialize with optional LLM client for memory extraction."""
        self.llm_client = llm_client
    
    def extract_memories_from_conversation(
        self, 
        conversation_data: Dict, 
        project_context: ProjectContext,
        conversation_file: str = ""
    ) -> List[Memory]:
        """Extract memories from a single conversation."""
        memories = []
        
        # Handle different conversation formats
        messages = self._extract_messages(conversation_data)
        
        for i, message in enumerate(messages):
            if self._is_user_message(message):
                # Extract memory from user message
                content = self._extract_message_content(message)
                if content and len(content.strip()) > 10:  # Filter out very short messages
                    memory = Memory(
                        content=content,
                        source_project=project_context.project_name,
                        conversation_file=conversation_file,
                        timestamp=self._extract_timestamp(message, i),
                        confidence_score=0.8,  # Default confidence
                        memory_type=self._classify_memory_type(content)
                    )
                    memories.append(memory)
        
        return memories
    
    def _extract_messages(self, conversation_data: Dict) -> List[Dict]:
        """Extract messages from conversation data in various formats."""
        # Handle common conversation formats
        if 'messages' in conversation_data:
            return conversation_data['messages']
        elif 'conversation' in conversation_data:
            return conversation_data['conversation']
        elif isinstance(conversation_data, list):
            return conversation_data
        else:
            # Try to find any list-like structure
            for key, value in conversation_data.items():
                if isinstance(value, list) and len(value) > 0:
                    # Check if it looks like a message list
                    if isinstance(value[0], dict) and any(
                        k in value[0] for k in ['content', 'message', 'text', 'role']
                    ):
                        return value
            return []
    
    def _is_user_message(self, message: Dict) -> bool:
        """Check if message is from the user (not assistant/system)."""
        role = message.get('role', '').lower()
        return role in ['user', 'human'] or (
            role == '' and 'user' in str(message.get('content', '')).lower()
        )
    
    def _extract_message_content(self, message: Dict) -> str:
        """Extract text content from a message."""
        # Try different common content fields
        for field in ['content', 'text', 'message', 'body']:
            if field in message:
                content = message[field]
                if isinstance(content, str):
                    return content.strip()
                elif isinstance(content, dict):
                    # Handle nested content structures
                    if 'text' in content:
                        return content['text'].strip()
        return ""
    
    def _extract_timestamp(self, message: Dict, index: int) -> str:
        """Extract timestamp from message or use index as fallback."""
        for field in ['timestamp', 'created_at', 'time']:
            if field in message:
                return str(message[field])
        return f"message_{index}"
    
    def _classify_memory_type(self, content: str) -> str:
        """Classify the type of memory based on content."""
        content_lower = content.lower()
        
        if any(tech_word in content_lower for tech_word in [
            'code', 'function', 'api', 'bug', 'error', 'implement', 'fix'
        ]):
            return "technical"
        elif any(pref_word in content_lower for pref_word in [
            'prefer', 'like', 'dislike', 'want', 'need', 'style'
        ]):
            return "preference"
        elif any(behav_word in content_lower for behav_word in [
            'usually', 'always', 'never', 'typically', 'often'
        ]):
            return "behavioral"
        else:
            return "general"