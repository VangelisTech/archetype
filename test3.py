# -*- coding: utf-8 -*-
"""
Test script for the Daft/Ray ECS implementation using RayDagSystem.
Now with a more complete 3D 6DOF physics implementation.
"""

import time
import daft
from daft import col, lit, DataType, udf, Series
import daft.expressions as F
from dataclasses import dataclass, field
import random
import math
import numpy as np # For math operations in UDFs if needed

# --- Ray Initialization ---
# Ensure Ray is initialized. Let RayDagSystem handle it by default,
# or initialize explicitly here if specific config needed.
# import ray
# ray.init(ignore_reinit_error=True, num_cpus=4)

# --- ECS Core Imports ---

    from core.base import Component, Processor, System
    from core.store import ComponentStore
    from core.managers import EcsQueryManager, EcsUpdateManager
    from core.world import EcsWorld
    from core.systems import RayDagSystem



# --- 3D 6DOF Components ---

@dataclass
class Position(Component): # Now 3D
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0

@dataclass
class Velocity(Component): # Linear Velocity (3D)
    vx: float = 0.0
    vy: float = 0.0
    vz: float = 0.0

@dataclass
class Orientation(Component): # Quaternion (w, x, y, z)
    w: float = 1.0
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0

@dataclass
class AngularVelocity(Component): # Radians per second (world frame or body frame? Let's assume body frame for Euler's)
    wx: float = 0.0
    wy: float = 0.0
    wz: float = 0.0

@dataclass
class PhysicsProperties(Component):
    mass: float = 1.0
    inv_mass: float = 1.0 # Precompute inverse mass
    # Diagonal Inertia Tensor (body frame)
    ix: float = 1.0
    iy: float = 1.0
    iz: float = 1.0
    # Precomputed inverse inertia tensor components
    inv_ix: float = 1.0
    inv_iy: float = 1.0
    inv_iz: float = 1.0

    def __post_init__(self):
        # Ensure inverse mass/inertia are calculated if mass/inertia are set
        if self.mass > 1e-9: self.inv_mass = 1.0 / self.mass
        else: self.inv_mass = 0.0 # Treat as infinite mass if mass is near zero
        if self.ix > 1e-9: self.inv_ix = 1.0 / self.ix
        else: self.inv_ix = 0.0
        if self.iy > 1e-9: self.inv_iy = 1.0 / self.iy
        else: self.inv_iy = 0.0
        if self.iz > 1e-9: self.inv_iz = 1.0 / self.iz
        else: self.inv_iz = 0.0


@dataclass
class AccumulatedForce(Component): # Net force this step (world frame)
    fx: float = 0.0
    fy: float = 0.0
    fz: float = 0.0

@dataclass
class AccumulatedTorque(Component): # Net torque this step (body frame)
    tx: float = 0.0
    ty: float = 0.0
    tz: float = 0.0

@dataclass
class StepStats(Component):
    step_count: int = 0


# --- Math Helpers / UDFs ---

# UDF for more complex quaternion integration (required for angular velocity)
# Input orientation: Struct(w,x,y,z)
# Input angular_velocity: Struct(wx,wy,wz) (body frame)
# Input dt: Float64
# Output: Struct(w,x,y,z)
@udf(return_dtype=DataType.struct({
    "w": DataType.float64(), "x": DataType.float64(),
    "y": DataType.float64(), "z": DataType.float64()
}))
def integrate_orientation_udf(orientations: Series, ang_velocities: Series, dt_series: Series):
    results = []
    orient_list = orientations.to_pylist()
    ang_vel_list = ang_velocities.to_pylist()
    # Assume dt is scalar for the batch
    dt = dt_series.to_pylist()[0] if dt_series else 0.0

    for i in range(len(orient_list)):
        q = orient_list[i]      # {'w': qw, 'x': qx, ...}
        omega = ang_vel_list[i] # {'wx': wx, 'wy': wy, ...}

        if q is None or omega is None:
             results.append(None)
             continue

        # Quaternion derivative: q_dot = 0.5 * W * q
        # Where W is the quaternion [0, omega_x, omega_y, omega_z]
        # Simplified integration: q_new = q + q_dot * dt
        half_dt = 0.5 * dt
        qw, qx, qy, qz = q['w'], q['x'], q['y'], q['z']
        wx, wy, wz = omega['wx'], omega['wy'], omega['wz']

        # Calculate derivative components (note: this order matters)
        qw_dot = -half_dt * (wx * qx + wy * qy + wz * qz)
        qx_dot =  half_dt * (wx * qw + wy * qz - wz * qy)
        qy_dot =  half_dt * (wy * qw - wx * qz + wz * qx)
        qz_dot =  half_dt * (wz * qw + wx * qy - wy * qx)

        # Update quaternion
        new_qw = qw + qw_dot
        new_qx = qx + qx_dot
        new_qy = qy + qy_dot
        new_qz = qz + qz_dot

        # Normalize
        norm_sq = new_qw**2 + new_qx**2 + new_qy**2 + new_qz**2
        if norm_sq < 1e-9:
            # Reset to identity if magnitude is near zero
            results.append({'w': 1.0, 'x': 0.0, 'y': 0.0, 'z': 0.0})
        else:
            inv_norm = 1.0 / math.sqrt(norm_sq)
            results.append({
                'w': new_qw * inv_norm, 'x': new_qx * inv_norm,
                'y': new_qy * inv_norm, 'z': new_qz * inv_norm
            })

    return results

# --- Granular 3D Processors ---

class ForceTorqueProcessor(Processor):
    """Calculates forces and torques acting on entities."""
    priority = 30 # Run first

    def process(self, querier: EcsQueryManager, updater: EcsUpdateManager, dt: float, *args, **kwargs) -> None:
        # Get entities with physics properties (needed for mass->gravity)
        entities_plan = querier.get_components(PhysicsProperties) # Only need this to know who exists

        # Skip if no entities have physics properties
        try:
            if len(entities_plan.collect()) == 0: return
        except Exception as e:
            print(f"ForceTorqueProcessor: Error collecting entities plan (might be empty): {e}")
            return # Assume empty if collection fails

        df = entities_plan
        props = col("physics_properties")
        mass = props["mass"]

        # --- Calculate Forces (World Frame) ---
        g = 9.8
        # Gravity force
        force_fx = lit(0.0)
        force_fy = mass * lit(-g) # Gravity acts downwards in Y
        force_fz = lit(0.0)
        # Add other forces here (drag, thrust, etc.) if needed

        df = df.with_column("final_force_fx", force_fx)
        df = df.with_column("final_force_fy", force_fy)
        df = df.with_column("final_force_fz", force_fz)

        # --- Calculate Torques (Body Frame) ---
        const_torque_z = 0.05 # Apply a small constant torque for testing
        # Simple angular damping (proportional to negative angular velocity)
        # Requires AngularVelocity component - need to join? Or apply in integration step?
        # Let's keep it simple: just apply constant torque here.
        torque_tx = lit(0.0)
        torque_ty = lit(0.0)
        torque_tz = lit(const_torque_z)

        df = df.with_column("final_torque_tx", torque_tx)
        df = df.with_column("final_torque_ty", torque_ty)
        df = df.with_column("final_torque_tz", torque_tz)

        # --- Create Update Structs ---
        force_struct = F.struct({
            "fx": col("final_force_fx"), "fy": col("final_force_fy"), "fz": col("final_force_fz")
        }).alias("accumulated_force")

        torque_struct = F.struct({
            "tx": col("final_torque_tx"), "ty": col("final_torque_ty"), "tz": col("final_torque_tz")
        }).alias("accumulated_torque")

        # --- Queue Updates ---
        entity_id_col = col("entity_id")
        updater.add_update(AccumulatedForce, df.select(entity_id_col, force_struct))
        updater.add_update(AccumulatedTorque, df.select(entity_id_col, torque_struct))
        # print("ForceTorqueProcessor finished.") # Reduce noise

class LinearMotionProcessor(Processor):
    """Integrates linear velocity and position from forces."""
    priority = 20 # Run after forces

    def process(self, querier: EcsQueryManager, updater: EcsUpdateManager, dt: float, *args, **kwargs) -> None:
        required = [Position, Velocity, AccumulatedForce, PhysicsProperties]
        entities_plan = querier.get_components(*required)

        try:
            if len(entities_plan.collect()) == 0: return
        except Exception as e:
            print(f"LinearMotionProcessor: Error collecting entities plan: {e}")
            return

        df = entities_plan
        dt_lit = lit(dt)

        pos = col("position")
        vel = col("velocity")
        force = col("accumulated_force")
        props = col("physics_properties")
        inv_mass = props["inv_mass"] # Use precomputed inverse mass

        # Linear Acceleration: a = F * inv_m (world frame)
        accel_x = force["fx"] * inv_mass
        accel_y = force["fy"] * inv_mass
        accel_z = force["fz"] * inv_mass

        # Integrate Velocity: v_new = v_old + a * dt
        new_vx = vel["vx"] + accel_x * dt_lit
        new_vy = vel["vy"] + accel_y * dt_lit
        new_vz = vel["vz"] + accel_z * dt_lit

        # Integrate Position: p_new = p_old + v_old * dt (Simple Euler)
        new_x = pos["x"] + vel["vx"] * dt_lit
        new_y = pos["y"] + vel["vy"] * dt_lit
        new_z = pos["z"] + vel["vz"] * dt_lit

        # --- Create Update Structs ---
        pos_update = F.struct({"x": new_x, "y": new_y, "z": new_z}).alias("position")
        vel_update = F.struct({"vx": new_vx, "vy": new_vy, "vz": new_vz}).alias("velocity")

        # --- Queue Updates ---
        entity_id_col = col("entity_id")
        updater.add_update(Position, df.select(entity_id_col, pos_update))
        updater.add_update(Velocity, df.select(entity_id_col, vel_update))
        # print("LinearMotionProcessor finished.") # Reduce noise


class AngularMotionProcessor(Processor):
    """Integrates angular velocity and orientation from torques."""
    priority = 15 # Run after forces/torques calculated

    def process(self, querier: EcsQueryManager, updater: EcsUpdateManager, dt: float, *args, **kwargs) -> None:
        required = [Orientation, AngularVelocity, AccumulatedTorque, PhysicsProperties]
        entities_plan = querier.get_components(*required)

        try:
            if len(entities_plan.collect()) == 0: return
        except Exception as e:
            print(f"AngularMotionProcessor: Error collecting entities plan: {e}")
            return

        df = entities_plan
        dt_lit = lit(dt)

        orient = col("orientation")
        ang_vel = col("angular_velocity") # Assume body frame ω
        torque = col("accumulated_torque") # Assume body frame τ
        props = col("physics_properties")
        # Diagonal inertia tensor I and its inverse
        Ix, Iy, Iz = props["ix"], props["iy"], props["iz"]
        inv_Ix, inv_Iy, inv_Iz = props["inv_ix"], props["inv_iy"], props["inv_iz"]
        # Angular velocity components in body frame
        wx, wy, wz = ang_vel["wx"], ang_vel["wy"], ang_vel["wz"]

        # Euler's equations for angular acceleration α (body frame)
        # α = I^-1 * (τ - ω x (Iω))
        # For diagonal I:
        # α_x = inv_Ix * (τ_x - (Iy - Iz) * wy * wz)
        # α_y = inv_Iy * (τ_y - (Iz - Ix) * wz * wx)
        # α_z = inv_Iz * (τ_z - (Ix - Iy) * wx * wy)

        gyro_term_x = (Iy - Iz) * wy * wz
        gyro_term_y = (Iz - Ix) * wz * wx
        gyro_term_z = (Ix - Iy) * wx * wy

        alpha_x = inv_Ix * (torque["tx"] - gyro_term_x)
        alpha_y = inv_Iy * (torque["ty"] - gyro_term_y)
        alpha_z = inv_Iz * (torque["tz"] - gyro_term_z)

        # Integrate Angular Velocity: w_new = w_old + alpha * dt
        new_wx = wx + alpha_x * dt_lit
        new_wy = wy + alpha_y * dt_lit
        new_wz = wz + alpha_z * dt_lit

        # Integrate Orientation: q_new = integrate(q_old, w_old, dt)
        # Use the UDF as quaternion integration is complex for pure expressions
        # We need the *old* angular velocity for integration step
        current_ang_vel_struct = F.struct({"wx": wx, "wy": wy, "wz": wz})
        new_orientation_expr = integrate_orientation_udf(orient, current_ang_vel_struct, dt_lit)

        # --- Create Update Structs ---
        ang_vel_update = F.struct({"wx": new_wx, "wy": new_wy, "wz": new_wz}).alias("angular_velocity")
        orient_update = new_orientation_expr.alias("orientation") # Result of UDF

        # --- Queue Updates ---
        entity_id_col = col("entity_id")
        updater.add_update(AngularVelocity, df.select(entity_id_col, ang_vel_update))
        updater.add_update(Orientation, df.select(entity_id_col, orient_update))
        # print("AngularMotionProcessor finished.") # Reduce noise


class StatsProcessor(Processor):
    """Updates simple step count."""
    priority = 0 # Run last

    def process(self, querier: EcsQueryManager, updater: EcsUpdateManager, dt: float, *args, **kwargs) -> None:
        entities_plan = querier.get_components(StepStats)

        try:
            if len(entities_plan.collect()) == 0: return
        except Exception as e:
            print(f"StatsProcessor: Error collecting entities plan: {e}")
            return

        df = entities_plan
        stats = col("step_stats")
        new_step_count = stats["step_count"] + 1
        stats_update = F.struct({"step_count": new_step_count}).alias("step_stats")

        updater.add_update(StepStats, df.select(col("entity_id"), stats_update))
        # print("StatsProcessor finished.") # Reduce noise


# --- Main Test Execution ---

if __name__ == "__main__":
    print("--- Starting ECS Test with RayDagSystem (3D 6DOF) ---")

    # 1. Initialize System
    ray_dag_system = RayDagSystem() # Assumes Ray init happens inside or is done externally

    # 2. Initialize World
    world = EcsWorld(system=ray_dag_system)

    # 3. Register Component Types
    component_types = [
        Position, Velocity, Orientation, AngularVelocity, PhysicsProperties,
        AccumulatedForce, AccumulatedTorque, StepStats
    ]
    for ct in component_types:
        world.register_component(ct)

    # 4. Register Entity Type
    world.create_entity_type("RigidBody3D", allowed_components=set(component_types))

    # 5. Add Processors to the World/System
    world.add_processor(ForceTorqueProcessor(), priority=30)
    world.add_processor(LinearMotionProcessor(), priority=20)
    world.add_processor(AngularMotionProcessor(), priority=15)
    world.add_processor(StatsProcessor(), priority=0)

    # 6. Create Entities
    num_entities = 3
    entity_ids = []
    print(f"\n--- Creating {num_entities} entities ---")
    for i in range(num_entities):
        entity_id = world.create_typed_entity(
            "RigidBody3D",
            Position(x=i*2.0, y=10.0, z=0.0),
            Velocity(vx=0.0, vy=0.0, vz=0.0),
            Orientation(), # Identity quat
            AngularVelocity(wx=0.0, wy=0.0, wz=random.uniform(-0.2, 0.2)*(i+1)), # Initial spin
            PhysicsProperties(mass=1.0 + i, ix=1.0, iy=1.1, iz=1.2), # Sets inv_mass etc
            AccumulatedForce(),
            AccumulatedTorque(),
            StepStats()
        )
        entity_ids.append(entity_id)

    # 7. Run the simulation loop
    num_steps = 5
    time_step = 0.1
    print(f"\n--- Running simulation for {num_steps} steps (dt={time_step}) ---")

    # Run one initial step to commit initial components & calculate initial forces/torques
    print("\n--- Running Initial Setup Step (dt=0.0) ---")
    world.process(dt=0.0)

    # Print initial state (after setup step)
    print("\n--- Initial State (After Setup) ---")
    components_to_show = [Position, Velocity, Orientation, AngularVelocity, AccumulatedForce, AccumulatedTorque, StepStats]
    for comp_type in components_to_show:
        df = world.get_component(comp_type)
        print(f"\nInitial {comp_type.__name__}:")
        if df is not None: df.show(num_entities)
        else: print(" (None)")
    print("-" * 30)

    # Run the main loop
    for step in range(num_steps):
        world.process(dt=time_step)
        # Reduced noise inside process() method now

    # 8. Verify Final State
    print(f"\n--- Final State after {num_steps} steps ---")

    for comp_type in components_to_show:
        df_final = world._querier.get_component(comp_type) # Use querier for final committed state
        print(f"\nFinal {comp_type.__name__}:")
        if df_final is not None: df_final.show(num_entities)
        else: print(" (None)")
    print("-" * 30)

    # Verify one entity
    if entity_ids:
        test_entity_id = entity_ids[0]
        print(f"\n--- Verifying Entity {test_entity_id} (Final State) ---")
        final_stats = world.component_for_entity(test_entity_id, StepStats)
        if final_stats:
            expected_steps = num_steps # Each process call runs the stats processor once
            print(f"  Stats: {final_stats}")
            assert final_stats.step_count == expected_steps, f"Expected {expected_steps} steps but got {final_stats.step_count}"
            print("  Stats step count verified.")
        else:
            print("  Could not retrieve final Stats.")

    print("\n--- Test Complete ---")
