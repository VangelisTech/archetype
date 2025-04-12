# _temp_nx_inspect.py
import inspect
import types
import networkx as nx
import json
import sys # Import sys to handle potential write errors

def extract_module_functions(module):
    """
    Extracts top-level functions from a module using inspect,
    and returns a dict of function names to their signatures as strings.
    Includes basic error handling for signature extraction.
    """
    function_signatures = {}
    # Iterate through members, checking if they are functions/builtins
    for name, obj in inspect.getmembers(module):
        if isinstance(obj, (types.FunctionType, types.BuiltinFunctionType)):
            try:
                # Attempt to get the signature
                signature = inspect.signature(obj)
                function_signatures[name] = str(signature)
            except (ValueError, TypeError) as e:
                # If signature can't be retrieved (e.g., some C extensions),
                # store a placeholder or skip. Let's store a placeholder.
                # print(f"Could not get signature for {name}: {e}", file=sys.stderr) # Print errors to stderr
                function_signatures[name] = "(*args, **kwargs)" # Placeholder
            except Exception as e:
                # Catch any other unexpected errors during inspection
                print(f"Unexpected error inspecting {name}: {e}", file=sys.stderr) # Print errors to stderr
                continue # Skip this member
    return function_signatures

# Apply to networkx
nx_signatures = extract_module_functions(nx)

# Output as JSON to stdout
try:
    print(json.dumps(nx_signatures, indent=2))
except IOError as e:
    # Handle potential broken pipe errors if output is large and piped
    if e.errno == 32: # errno.EPIPE
        pass # Ignore broken pipe error
    else:
        raise # Re-raise other IOErrors 