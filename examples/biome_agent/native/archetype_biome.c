// Copyright 2026 Vangelis Technologies Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * Example-local native bridge into Sander Mertens' Biome.
 *
 * This module intentionally delegates both economy and construction to Biome.
 * It is copied into the pinned, gitignored checkout by bootstrap.py and linked
 * into the upstream executable; no upstream source or asset is vendored.
 */

#include "biome.h"

static bool archetype_biome_validName(const char *name) {
    if (!name || !name[0]) {
        return false;
    }
    char ch = name[0];
    if (!((ch >= 'A' && ch <= 'Z') ||
          (ch >= 'a' && ch <= 'z') || ch == '_'))
    {
        return false;
    }
    for (int32_t i = 1; name[i]; i ++) {
        ch = name[i];
        if (!((ch >= 'A' && ch <= 'Z') ||
              (ch >= 'a' && ch <= 'z') ||
              (ch >= '0' && ch <= '9') || ch == '_'))
        {
            return false;
        }
    }
    return true;
}

static void archetype_biome_placeBuilding(
    const ecs_function_ctx_t *ctx,
    int32_t argc,
    const ecs_value_t *argv,
    ecs_value_t *result)
{
    int64_t *result_id = result->ptr;
    *result_id = 0;

    if (argc != 5) {
        return;
    }

    ecs_world_t *world = ctx->world;
    ecs_entity_t prefab = *(ecs_entity_t*)argv[0].ptr;
    ecs_entity_t terrain = *(ecs_entity_t*)argv[1].ptr;
    int32_t x = *(int32_t*)argv[2].ptr;
    int32_t y = *(int32_t*)argv[3].ptr;
    const char *name = *(ecs_string_t*)argv[4].ptr;

    if (!archetype_biome_validName(name)) {
        return;
    }

    ecs_entity_t parent = ecs_lookup(world, "scene.buildings");
    if (!parent) {
        parent = terrain;
    }
    if (ecs_lookup_child(world, parent, name)) {
        return;
    }

    const BiomeBuilding *building = ecs_get(
        world, prefab, BiomeBuilding);
    if (!building) {
        return;
    }

    int32_t width = building->footprint.x > 1
        ? (int32_t)building->footprint.x
        : 1;
    int32_t height = building->footprint.y > 1
        ? (int32_t)building->footprint.y
        : 1;

    if (!biome_factory_purchase(world, prefab, 1)) {
        return;
    }

    ecs_entity_t placed = biomePlaceBuilding(
        world, prefab, terrain, x, y, width, height, 0);
    if (!placed) {
        biome_factory_refund(world, prefab, 1);
        return;
    }

    ecs_set_name(world, placed, name);
    *result_id = (int64_t)placed;
}

void archetypeBiomeImport(ecs_world_t *world) {
    ECS_MODULE(world, archetypeBiome);

    ECS_IMPORT(world, biomeFactory);
    ECS_IMPORT(world, biomeBuildings);

    ecs_entity_t module = ecs_id(archetypeBiome);
    ecs_function(world, {
        .name = "placeBuilding",
        .parent = module,
        .return_type = ecs_id(ecs_i64_t),
        .params = {
            { .name = "prefab", .type = ecs_id(ecs_entity_t) },
            { .name = "terrain", .type = ecs_id(ecs_entity_t) },
            { .name = "x", .type = ecs_id(ecs_i32_t) },
            { .name = "y", .type = ecs_id(ecs_i32_t) },
            { .name = "name", .type = ecs_id(ecs_string_t) }
        },
        .callback = archetype_biome_placeBuilding
    });
}
