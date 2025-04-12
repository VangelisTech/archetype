import os
import sys
import inspect

# Add the core directory to the Python path

import re
import ast
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Type, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("docstrings_precommit")

# Constants
PROJECT_ROOT = Path(__file__).parent.parent
CORE_DIR = PROJECT_ROOT / "core"
DOCS_DIR = PROJECT_ROOT / "docs"

# Add the project root to the Python path
sys.path.insert(0, str(PROJECT_ROOT))


class DocstringUpdater:
    """
    Updates docstrings for classes based on their signatures and example patterns.
    Maintains a repository of class structures and synchronizes with documentation generation.
    """
    
    def __init__(self, root_dir: Path = PROJECT_ROOT):
        self.root_dir = root_dir
        self.class_info: Dict[str, Dict[str, Any]] = {}
        self.modified_files: Set[Path] = set()
    
    def scan_python_files(self, directory: Path) -> List[Path]:
        """
        Recursively scans a directory for Python files.
        
        Args:
            directory: The directory to scan
            
        Returns:
            List of Path objects for Python files
        """
        return list(directory.glob("**/*.py"))
    
    def parse_file(self, file_path: Path) -> Optional[ast.Module]:
        """
        Parses a Python file into an AST.
        
        Args:
            file_path: Path to the Python file
            
        Returns:
            AST Module or None if parsing fails
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            return ast.parse(content, filename=str(file_path))
        except Exception as e:
            logger.error(f"Failed to parse {file_path}: {e}")
            return None
    
    def extract_class_info(self, module: ast.Module, file_path: Path) -> None:
        """
        Extracts class information from an AST module.
        
        Args:
            module: AST module to analyze
            file_path: Path to the source file
        """
        for node in ast.walk(module):
            if isinstance(node, ast.ClassDef):
                class_name = node.name
                full_path = f"{file_path.relative_to(self.root_dir)}:{class_name}"
                
                # Extract docstring
                docstring = ast.get_docstring(node)
                
                # Extract methods
                methods = []
                for child in node.body:
                    if isinstance(child, ast.FunctionDef):
                        method_name = child.name
                        method_docstring = ast.get_docstring(child)
                        method_args = [arg.arg for arg in child.args.args if arg.arg != 'self']
                        
                        methods.append({
                            "name": method_name,
                            "docstring": method_docstring,
                            "args": method_args
                        })
                
                # Extract base classes
                base_classes = []
                for base in node.bases:
                    if isinstance(base, ast.Name):
                        base_classes.append(base.id)
                    elif isinstance(base, ast.Attribute):
                        base_classes.append(f"{base.value.id}.{base.attr}")
                
                self.class_info[full_path] = {
                    "name": class_name,
                    "file_path": str(file_path),
                    "docstring": docstring,
                    "methods": methods,
                    "base_classes": base_classes
                }
    
    def generate_docstring(self, class_info: Dict[str, Any]) -> str:
        """
        Generates a docstring for a class based on its structure.
        
        Args:
            class_info: Dictionary containing class information
            
        Returns:
            Generated docstring
        """
        # Start with the existing docstring or create a basic one
        docstring = class_info.get("docstring", "") or f"{class_info['name']} class."
        
        # Add information about inheritance if applicable
        if class_info["base_classes"]:
            base_classes_str = ", ".join(class_info["base_classes"])
            inheritance_line = f"\nInherits from: {base_classes_str}"
            if inheritance_line not in docstring:
                docstring += inheritance_line
        
        # Add method summary if not already present
        if class_info["methods"] and "Methods:" not in docstring:
            docstring += "\n\nMethods:\n"
            for method in class_info["methods"]:
                args_str = ", ".join(method["args"])
                method_desc = method["docstring"].split("\n")[0] if method["docstring"] else "No description"
                docstring += f"    {method['name']}({args_str}): {method_desc}\n"
        
        return docstring
    
    def update_file_docstrings(self, file_path: Path) -> bool:
        """
        Updates docstrings in a file based on extracted class information.
        
        Args:
            file_path: Path to the file to update
            
        Returns:
            True if file was modified, False otherwise
        """
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        original_content = content
        
        # Find classes in this file
        file_classes = {k: v for k, v in self.class_info.items() 
                        if Path(v["file_path"]) == file_path}
        
        for class_key, class_info in file_classes.items():
            class_name = class_info["name"]
            new_docstring = self.generate_docstring(class_info)
            
            # Create pattern to find class definition and its docstring
            class_pattern = rf"class\s+{class_name}\s*\([^)]*\)\s*:"
            match = re.search(class_pattern, content)
            
            if match:
                # Find the position after the class definition
                pos = match.end()
                
                # Check if there's an existing docstring
                docstring_match = re.search(r'^\s*""".*?"""', content[pos:], re.DOTALL)
                
                if docstring_match:
                    # Replace existing docstring
                    start = pos + docstring_match.start()
                    end = pos + docstring_match.end()
                    content = content[:start] + f'    """{new_docstring}"""' + content[end:]
                else:
                    # Add new docstring after class definition
                    content = content[:pos] + f'\n    """{new_docstring}"""' + content[pos:]
        
        # Check if content was modified
        if content != original_content:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            return True
        
        return False
    
    def run(self) -> None:
        """
        Runs the docstring update process for the entire project.
        """
        logger.info("Starting docstring update process")
        
        # Scan for Python files
        python_files = self.scan_python_files(self.root_dir)
        logger.info(f"Found {len(python_files)} Python files")
        
        # Extract class information
        for file_path in python_files:
            module = self.parse_file(file_path)
            if module:
                self.extract_class_info(module, file_path)
        
        logger.info(f"Extracted information for {len(self.class_info)} classes")
        
        # Update docstrings
        for file_path in python_files:
            if self.update_file_docstrings(file_path):
                self.modified_files.add(file_path)
        
        logger.info(f"Updated docstrings in {len(self.modified_files)} files")
        
        # Generate documentation if needed
        self.generate_documentation()
    
    def generate_documentation(self) -> None:
        """
        Generates documentation based on the extracted class information.
        """
        # Ensure docs directory exists
        DOCS_DIR.mkdir(exist_ok=True)
        
        # Group classes by module
        modules: Dict[str, List[Dict[str, Any]]] = {}
        for class_key, class_info in self.class_info.items():
            file_path = Path(class_info["file_path"])
            module_path = str(file_path.relative_to(self.root_dir)).replace("/", ".").replace(".py", "")
            
            if module_path not in modules:
                modules[module_path] = []
            
            modules[module_path].append(class_info)
        
        # Generate module documentation
        for module_path, classes in modules.items():
            doc_path = DOCS_DIR / f"{module_path.replace('.', '/')}.md"
            doc_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(doc_path, "w", encoding="utf-8") as f:
                f.write(f"# {module_path}\n\n")
                
                for class_info in classes:
                    f.write(f"## {class_info['name']}\n\n")
                    f.write(f"{class_info['docstring']}\n\n")
                    
                    if class_info["methods"]:
                        f.write("### Methods\n\n")
                        for method in class_info["methods"]:
                            f.write(f"#### {method['name']}\n\n")
                            if method["docstring"]:
                                f.write(f"{method['docstring']}\n\n")
                            else:
                                f.write("No description available.\n\n")
        
        logger.info(f"Generated documentation in {DOCS_DIR}")


def main():
    """
    Main entry point for the docstring update pre-commit hook.
    """
    updater = DocstringUpdater()
    updater.run()


if __name__ == "__main__":
    main()
