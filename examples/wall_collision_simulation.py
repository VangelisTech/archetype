from archetype import Component, Processor, processor, make_simple_world
from daft import col, DataFrame, lit
import pyglet

class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

class Wall(Component):
    min_x: float = 0.0
    max_x: float = 100.0
    min_y: float = 0.0
    max_y: float = 100.0

class Ball(Component):
    def __init__(self, image: str, radius: float = 5.0):
        self.sprite = pyglet.sprite.Sprite(image, x = radius, y = radius)
        self.radius = radius

@processor(Position, Velocity, priority=10)
class MovementProcessor(Processor):
    def process(self, df: DataFrame, dt: float) -> DataFrame:
        df = df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })
        return df
    
@processor(Position, Velocity, Ball, priority=1)
class WallCollisionProcessor(Processor):


    def process(self, df: DataFrame, dt: float, wall: Wall = Wall(min_x=0, max_x=100, min_y=0, max_y=100)) -> DataFrame:
        # Figure out which entities are colliding with the walls
        df = df.with_columns({
            "hit_left_wall":   col("position__x") - col("ball__radius") < wall.min_x,
            "hit_right_wall":  col("position__x") + col("ball__radius") > wall.max_x,
            "hit_bottom_wall": col("position__y") - col("ball__radius") < wall.min_y,
            "hit_top_wall":    col("position__y") + col("ball__radius") > wall.max_y,
            "is_colliding_x":  col("hit_left_wall") | col("hit_right_wall"),
            "is_colliding_y":  col("hit_bottom_wall") | col("hit_top_wall"),
            "velocity__vx":    col("is_colliding_x").if_else(col("velocity__vx") * -1, col("velocity__vx")),
            "velocity__vy":    col("is_colliding_y").if_else(col("velocity__vy") * -1, col("velocity__vy")),
        })
        return df
    


def main(uri, debug=False):
    world = make_simple_world(uri, debug=debug)



    movement_processor = MovementProcessor()
    wall_collision_processor = WallCollisionProcessor()


    world.add_processor(movement_processor)
    world.add_processor(WallCollisionProcessor())
    
    

    # Set up Pyglet
    window = pyglet.window.Window(
        width=720,
        height=480,
        caption="Archetype Wall Collision Pyglet example"
    )
    batch = pyglet.graphics.Batch()
    red_ball_image = pyglet.resource.image("assets/red_ball.png")
    blue_ball_image = pyglet.resource.image("assets/blue_ball.png")

    red_enemy =world.spawn_entity(
        Ball(sprite=pyglet.sprite.Sprite(red_ball_image, x=5, y=5)), 
        Position(x=10, y=10), 
        Velocity(vx=4.26, vy= 10.33)
    )
    blue_player = world.spawn_entity(
        Ball(sprite=pyglet.sprite.Sprite(blue_ball_image, x=50, y=50)),
        Position(x=50, y=50), 
        Velocity(vx=0, vy=0)
    )
    

    ################################################
    #  Set up pyglet events for input and rendering:
    ################################################

    @window.event
    def on_key_press(key, mod):
        df = world.archetype_for_entity(blue_player, Velocity, step=[-1])
        df = df.with_columns({
            "velocity__vx": 
                lit(3).if_else(key == pyglet.window.key.RIGHT,
                    lit(-3).if_else(key == pyglet.window.key.LEFT, lit(0))
            ),
            "velocity__vy": 
                lit(3).if_else(key == pyglet.window.key.UP, 
                    lit(-3).if_else(key == pyglet.window.key.DOWN, lit(0))
            ),
        })
        return df

    @window.event
    def on_key_release(key, mod):
        df = world.archetype_for_entity(blue_player, Velocity, step=[-1])
        df = df.with_columns({
            "velocity__vx": lit(0).if_else(key in (pyglet.window.key.RIGHT, pyglet.window.key.LEFT), col("velocity__vx")),
            "velocity__vy": lit(0).if_else(key in (pyglet.window.key.UP, pyglet.window.key.DOWN), col("velocity__vy"))
        })
        return df

    @window.event
    def on_draw():
        # Clear the window:
        window.clear()
        # Draw the batch of Renderables:
        batch.draw()


    @processor(Velocity, priority=1)
    
        
        





if __name__ == "__main__":
    # NOTE!  schedule_interval will automatically pass a "delta time" argument
    #        to esper.process, so you must make sure that your Processor classes
    #        account for this. See the example Processors above.

    FPS = 20
    pyglet.clock.schedule_interval(MovementProcessor().process, interval=1.0/FPS)
    pyglet.clock.schedule_interval(PlayerInputProcessor().process, interval=1.0/FPS)
    pyglet.app.run()