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

# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Initial Condition Samplers
==========================

Sample initial conditions for Monte Carlo analysis and episode generation.
This is the flight controls perspective: systematic coverage of the state space.

Samplers provide initial entity states for parallel episode execution,
enabling embarrassingly parallel rollouts for dataset curation.
"""

import random
from typing import Protocol, runtime_checkable

from archetype.core import Component


@runtime_checkable
class InitialConditionSampler(Protocol):
    """
    Protocol for sampling initial conditions.

    Samplers generate lists of component instances that define
    the initial state for entities in an episode.
    """

    def sample(self, n: int, seed: int | None = None) -> list[list[Component]]:
        """
        Sample n sets of initial conditions.

        Args:
            n: Number of initial condition sets to generate
            seed: Optional random seed for reproducibility

        Returns:
            List of component lists, one per entity/episode
        """
        ...


class UniformSampler:
    """
    Uniform random sampling over component field bounds.

    Samples each field uniformly within specified bounds.
    Simple but can miss important regions of state space.

    Example:
        sampler = UniformSampler(
            component_type=State,
            bounds={
                "x": (-10.0, 10.0),
                "y": (-10.0, 10.0),
                "vx": (-1.0, 1.0),
                "vy": (-1.0, 1.0),
            }
        )
        initial_conditions = sampler.sample(100, seed=42)
    """

    def __init__(
        self,
        component_type: type[Component],
        bounds: dict[str, tuple[float, float]],
        additional_components: list[Component] | None = None,
    ):
        """
        Args:
            component_type: Component class to instantiate
            bounds: Dict mapping field names to (min, max) bounds
            additional_components: Optional fixed components to include with each sample
        """
        self.component_type = component_type
        self.bounds = bounds
        self.additional_components = additional_components or []

    def sample(self, n: int, seed: int | None = None) -> list[list[Component]]:
        """Sample n sets of initial conditions uniformly."""
        if seed is not None:
            random.seed(seed)

        samples = []
        for _ in range(n):
            # Sample each field uniformly
            field_values = {}
            for field_name, (low, high) in self.bounds.items():
                field_values[field_name] = random.uniform(low, high)

            # Create component instance
            component = self.component_type(**field_values)

            # Combine with additional fixed components
            samples.append([component] + list(self.additional_components))

        return samples


class LatinHypercubeSampler:
    """
    Latin Hypercube Sampling for efficient state space coverage.

    LHS ensures better coverage than uniform sampling by stratifying
    each dimension. Essential for flight controls linear analysis.

    Example:
        sampler = LatinHypercubeSampler(
            component_type=State,
            bounds={"x": (-10, 10), "y": (-10, 10)},
            divisions=10,  # 10 strata per dimension
        )
    """

    def __init__(
        self,
        component_type: type[Component],
        bounds: dict[str, tuple[float, float]],
        additional_components: list[Component] | None = None,
    ):
        self.component_type = component_type
        self.bounds = bounds
        self.additional_components = additional_components or []

    def sample(self, n: int, seed: int | None = None) -> list[list[Component]]:
        """Sample n sets using Latin Hypercube Sampling."""
        if seed is not None:
            random.seed(seed)

        field_names = list(self.bounds.keys())
        num_dims = len(field_names)

        # Generate LHS samples for each dimension
        # Divide [0, 1] into n equal strata, sample one point per stratum
        dim_samples = []
        for _ in range(num_dims):
            # Random permutation of strata
            perm = list(range(n))
            random.shuffle(perm)

            # Sample within each stratum
            samples_1d = []
            for i in perm:
                low = i / n
                high = (i + 1) / n
                samples_1d.append(random.uniform(low, high))

            dim_samples.append(samples_1d)

        # Convert to component instances
        samples = []
        for i in range(n):
            field_values = {}
            for j, field_name in enumerate(field_names):
                low, high = self.bounds[field_name]
                # Scale from [0, 1] to [low, high]
                field_values[field_name] = low + dim_samples[j][i] * (high - low)

            component = self.component_type(**field_values)
            samples.append([component] + list(self.additional_components))

        return samples


class GridSampler:
    """
    Grid-based sampling for systematic coverage.

    Creates a regular grid over the state space.
    Useful for visualization and deterministic analysis.
    """

    def __init__(
        self,
        component_type: type[Component],
        bounds: dict[str, tuple[float, float]],
        points_per_dim: int = 10,
        additional_components: list[Component] | None = None,
    ):
        self.component_type = component_type
        self.bounds = bounds
        self.points_per_dim = points_per_dim
        self.additional_components = additional_components or []

    def sample(self, n: int | None = None, seed: int | None = None) -> list[list[Component]]:
        """
        Sample on a grid.

        Note: n is ignored; grid size is determined by points_per_dim.
        """
        import itertools

        field_names = list(self.bounds.keys())

        # Generate grid points for each dimension
        dim_points = []
        for field_name in field_names:
            low, high = self.bounds[field_name]
            points = [
                low + i * (high - low) / (self.points_per_dim - 1)
                for i in range(self.points_per_dim)
            ]
            dim_points.append(points)

        # Cartesian product of all dimensions
        samples = []
        for combo in itertools.product(*dim_points):
            field_values = dict(zip(field_names, combo, strict=False))
            component = self.component_type(**field_values)
            samples.append([component] + list(self.additional_components))

        return samples


class TrimPointSampler:
    """
    Sample around trim/equilibrium points for linearization analysis.

    Flight controls perspective: sample perturbations around known
    equilibrium points to analyze local stability and controllability.

    Example:
        sampler = TrimPointSampler(
            component_type=State,
            trim_points=[
                {"x": 0.0, "y": 0.0, "vx": 0.0, "vy": 0.0},  # Hover
                {"x": 0.0, "y": 0.0, "vx": 10.0, "vy": 0.0}, # Cruise
            ],
            perturbation_bounds={
                "x": (-0.1, 0.1),
                "y": (-0.1, 0.1),
                "vx": (-0.5, 0.5),
                "vy": (-0.5, 0.5),
            },
        )
    """

    def __init__(
        self,
        component_type: type[Component],
        trim_points: list[dict[str, float]],
        perturbation_bounds: dict[str, tuple[float, float]],
        additional_components: list[Component] | None = None,
    ):
        self.component_type = component_type
        self.trim_points = trim_points
        self.perturbation_bounds = perturbation_bounds
        self.additional_components = additional_components or []

    def sample(self, n: int, seed: int | None = None) -> list[list[Component]]:
        """Sample perturbations around trim points."""
        if seed is not None:
            random.seed(seed)

        samples = []
        samples_per_trim = n // len(self.trim_points)

        for trim_point in self.trim_points:
            for _ in range(samples_per_trim):
                field_values = {}
                for field_name, trim_value in trim_point.items():
                    if field_name in self.perturbation_bounds:
                        delta_low, delta_high = self.perturbation_bounds[field_name]
                        perturbation = random.uniform(delta_low, delta_high)
                        field_values[field_name] = trim_value + perturbation
                    else:
                        field_values[field_name] = trim_value

                component = self.component_type(**field_values)
                samples.append([component] + list(self.additional_components))

        return samples


class CompositeSampler:
    """
    Combine multiple samplers for mixed sampling strategies.

    Example:
        sampler = CompositeSampler([
            (UniformSampler(...), 0.7),   # 70% uniform
            (TrimPointSampler(...), 0.3), # 30% around trim points
        ])
    """

    def __init__(self, samplers_with_weights: list[tuple["InitialConditionSampler", float]]):
        self.samplers = samplers_with_weights
        total_weight = sum(w for _, w in samplers_with_weights)
        self.normalized_weights = [w / total_weight for _, w in samplers_with_weights]

    def sample(self, n: int, seed: int | None = None) -> list[list[Component]]:
        """Sample from multiple samplers according to weights."""
        if seed is not None:
            random.seed(seed)

        samples = []
        for (sampler, _), weight in zip(self.samplers, self.normalized_weights, strict=False):
            num_samples = int(n * weight)
            samples.extend(sampler.sample(num_samples, seed=None))

        # Shuffle to mix samples from different samplers
        random.shuffle(samples)

        return samples[:n]  # Ensure exactly n samples
