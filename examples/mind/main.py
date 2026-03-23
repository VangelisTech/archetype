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

import argparse
from pathlib import Path
from typing import Optional

from .aggregator import MemoryAggregator
from .extractor import MemoryExtractor
from .scanner import ConversationScanner
from .storage import MemoryStorage


def main(
    base_path: Optional[Path] = None,
    incremental: bool = False,
    output_path: Optional[Path] = None
):
    """Main function to run the mind extraction process."""
    print("🧠 Mind Extraction: Processing all .claude project directories...")
    
    # Initialize components
    scanner = ConversationScanner(base_path)
    extractor = MemoryExtractor()
    aggregator = MemoryAggregator()
    storage = MemoryStorage()
    
    # Set up incremental processing
    last_run_file = None
    if incremental:
        last_run_file = storage.storage_path / "last_run.json"
        print(f"📅 Running in incremental mode (tracking file: {last_run_file})")
    
    # Scan and process conversations
    total_conversations = 0
    total_memories = 0
    
    conversation_generator = (
        scanner.get_new_conversations_since(last_run_file) if incremental
        else scanner.scan_all_projects()
    )
    
    for project_context, conversation_data in conversation_generator:
        try:
            # Extract memories from conversation
            memories = extractor.extract_memories_from_conversation(
                conversation_data, 
                project_context,
                conversation_file=f"{project_context.project_name}_conversation"
            )
            
            # Filter duplicates at the content level
            unique_memories = []
            for memory in memories:
                if not scanner.is_duplicate_content(memory.content):
                    unique_memories.append(memory)
            
            if unique_memories:
                aggregator.add_memories(unique_memories)
                total_memories += len(unique_memories)
                print(f"✅ Processed project '{project_context.project_name}': {len(unique_memories)} new memories")
            
            total_conversations += 1
            
        except Exception as e:
            print(f"❌ Error processing conversation in {project_context.project_name}: {e}")
            continue
    
    # Save memories to storage
    if total_memories > 0:
        saved_count = storage.save_memories(aggregator.all_memories)
        print(f"💾 Saved {saved_count} new memories to LanceDB")
    
    # Generate unified profile
    unified_profile = aggregator.get_unified_profile()
    
    # Export profile
    if output_path:
        export_path = storage.export_profile_to_json(unified_profile, output_path)
    else:
        export_path = storage.export_profile_to_json(unified_profile)
    
    # Print summary
    print(f"\n📊 Mind Extraction Complete!")
    print(f"   • Conversations processed: {total_conversations}")
    print(f"   • Memories extracted: {total_memories}")
    print(f"   • Total memories in storage: {storage.get_memory_count()}")
    print(f"   • Projects covered: {len(unified_profile['projects_covered'])}")
    print(f"   • Profile exported to: {export_path}")
    
    # Print quick insights
    print(f"\n🧠 Quick Insights:")
    memory_types = unified_profile['memory_types']
    for mem_type, count in memory_types.items():
        print(f"   • {mem_type.capitalize()}: {count} memories")
    
    if unified_profile['high_confidence_memories']:
        print(f"\n🔍 Sample High-Confidence Memory:")
        sample = unified_profile['high_confidence_memories'][0]
        print(f"   \"{sample['content'][:100]}...\" (Project: {sample['project']})")
    
    return unified_profile


def cli():
    """Command-line interface for mind extraction."""
    parser = argparse.ArgumentParser(description="Extract minds from all .claude project directories")
    parser.add_argument(
        "--base-path", 
        type=Path, 
        help="Base path to search for .claude directories (default: user home)"
    )
    parser.add_argument(
        "--incremental", 
        action="store_true", 
        help="Only process new conversations since last run"
    )
    parser.add_argument(
        "--output", 
        type=Path, 
        help="Output path for the unified profile JSON"
    )
    
    args = parser.parse_args()
    
    main(
        base_path=args.base_path,
        incremental=args.incremental,
        output_path=args.output
    )


if __name__ == "__main__":
    cli()