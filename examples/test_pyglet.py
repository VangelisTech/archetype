#!/usr/bin/env python3
"""
Simple test script to verify pyglet is installed and working
"""
import sys

try:
    import pyglet
    print(f"✓ Pyglet {pyglet.version} is installed")
    
    # Test window creation
    try:
        window = pyglet.window.Window(width=400, height=300, caption="Pyglet Test", visible=False)
        print("✓ Can create windows")
        window.close()
    except Exception as e:
        print(f"✗ Error creating window: {e}")
        sys.exit(1)
    
    # Test resource loading
    try:
        pyglet.resource.path = ['assets']
        pyglet.resource.reindex()
        red_ball = pyglet.resource.image('red_ball.png')
        blue_ball = pyglet.resource.image('blue_ball.png')
        print("✓ Can load image resources")
    except Exception as e:
        print(f"✗ Error loading resources: {e}")
        sys.exit(1)
    
    print("\nPyglet is working correctly! You can run the wall collision simulation.")
    
except ImportError:
    print("✗ Pyglet is not installed")
    print("\nPlease install it with:")
    print("  pip install pyglet")
    sys.exit(1) 