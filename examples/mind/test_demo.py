#!/usr/bin/env python3
"""Simple test demo for mind extraction functionality."""

import json
import tempfile
from pathlib import Path

from .components import Memory, ProjectContext, Dedup
from .scanner import ConversationScanner
from .extractor import MemoryExtractor
from .aggregator import MemoryAggregator
from .storage import MemoryStorage


def create_test_conversations(temp_dir: Path):
    """Create test conversation files."""
    # Create .claude/projects structure
    claude_dir = temp_dir / ".claude" / "projects"
    
    # Project A
    project_a = claude_dir / "project-a"
    project_a.mkdir(parents=True, exist_ok=True)
    
    conversation_a = {
        "messages": [
            {"role": "user", "content": "I prefer using TypeScript for all new projects"},
            {"role": "assistant", "content": "I'll help you set up TypeScript"},
            {"role": "user", "content": "Can you fix the bug in the authentication system?"}
        ]
    }
    
    with open(project_a / "conversation1.json", 'w') as f:
        json.dump(conversation_a, f)
    
    # Project B
    project_b = claude_dir / "project-b"
    project_b.mkdir(parents=True, exist_ok=True)
    
    conversation_b = {
        "messages": [
            {"role": "user", "content": "I always test my code thoroughly before deployment"},
            {"role": "assistant", "content": "That's a great practice"},
            {"role": "user", "content": "Help me implement a new feature for user management"}
        ]
    }
    
    with open(project_b / "conversation1.json", 'w') as f:
        json.dump(conversation_b, f)
    
    return claude_dir.parent.parent  # Return base path


def test_components():
    """Test individual components."""
    print("🧪 Testing Components...")
    
    # Test ProjectContext
    project_path = Path("/test/project-x")
    context = ProjectContext.from_project_path(project_path)
    assert context.project_name == "project-x"
    print("✅ ProjectContext working")
    
    # Test Dedup
    content1 = "This is a test message"
    content2 = "This is a test message"
    content3 = "This is a different message"
    
    dedup1 = Dedup.from_content(content1)
    dedup2 = Dedup.from_content(content2)
    dedup3 = Dedup.from_content(content3)
    
    assert dedup1 == dedup2
    assert dedup1 != dedup3
    print("✅ Dedup working")
    
    # Test Memory
    memory = Memory(
        content="Test memory",
        source_project="test-project",
        conversation_file="test.json",
        timestamp="2024-01-01",
        confidence_score=0.9,
        memory_type="technical"
    )
    assert memory.content == "Test memory"
    print("✅ Memory component working")


def test_full_pipeline():
    """Test the full extraction pipeline."""
    print("\n🧪 Testing Full Pipeline...")
    
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        base_path = create_test_conversations(temp_dir)
        
        print(f"📁 Created test conversations in {base_path}")
        
        # Initialize components
        scanner = ConversationScanner(base_path)
        extractor = MemoryExtractor()
        aggregator = MemoryAggregator()
        
        # Test scanner
        projects = scanner.find_claude_projects()
        assert len(projects) == 2
        print(f"✅ Found {len(projects)} projects")
        
        # Test extraction pipeline
        memory_count = 0
        for project_context, conversation_data in scanner.scan_all_projects():
            memories = extractor.extract_memories_from_conversation(
                conversation_data, project_context, "test.json"
            )
            
            # Filter duplicates
            unique_memories = []
            for memory in memories:
                if not scanner.is_duplicate_content(memory.content):
                    unique_memories.append(memory)
            
            aggregator.add_memories(unique_memories)
            memory_count += len(unique_memories)
            print(f"✅ Extracted {len(unique_memories)} memories from {project_context.project_name}")
        
        # Test aggregation
        profile = aggregator.get_unified_profile()
        assert profile['total_memories'] == memory_count
        assert len(profile['projects_covered']) == 2
        print(f"✅ Generated unified profile with {memory_count} memories")
        
        print("\n📊 Profile Summary:")
        print(f"   • Total memories: {profile['total_memories']}")
        print(f"   • Projects: {profile['projects_covered']}")
        print(f"   • Memory types: {profile['memory_types']}")


def main():
    """Run all tests."""
    print("🧠 Mind Extraction Test Demo\n")
    
    try:
        test_components()
        test_full_pipeline()
        
        print("\n✅ All tests passed! Mind extraction system is working correctly.")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    main()