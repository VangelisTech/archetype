# Required imports (assuming FastAPI and Pydantic)
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Type

# --- Placeholder for your actual ComponentStore/Service ---
# class ComponentStoreService:
#     def add_component(self, entity_id: int, component_type_name: str, data: Dict[str, Any], step: int):
#         # 1. Resolve component_type_name to actual type/schema
#         # 2. Validate data against schema
#         # 3. Calculate schema hash
#         # 4. Buffer/store data internally (associating with entity_id, step, schema_hash)
#         print(f"Adding: Entity {entity_id}, Type {component_type_name}, Step {step}, Data {data}")
#         pass # Replace with real logic

#     def commit(self) -> str:
#         # 1. Flush buffered data to LanceDB tables based on schema_hash
#         # 2. Update catalog if necessary
#         # 3. Return a commit identifier (e.g., hash or timestamp)
#         print("Committing data...")
#         commit_id = "dummy_commit_123" # Replace with real logic
#         return commit_id

# store_service = ComponentStoreService() # Instantiate your service
# --- End Placeholder ---

app = FastAPI(
    title="ECS Component Store API",
    description="API for ingesting and managing versioned component data.",
)

# --- Data Models for API Requests ---

class ComponentUpdate(BaseModel):
    """Represents a single component state update for an entity at a specific step."""
    entity_id: int = Field(..., description="The unique identifier for the entity.")
    component_type: str = Field(..., description="The string name of the component type (e.g., 'Position', 'Velocity').")
    data: Dict[str, Any] = Field(..., description="A dictionary containing the component's fields and values.")
    step: int = Field(..., description="The simulation step or timestamp for this component state.")

class BulkIngestRequest(BaseModel):
    """Request model for ingesting multiple component updates in a single batch."""
    updates: List[ComponentUpdate] = Field(..., description="A list of component updates.")

# --- API Endpoints ---

@app.post(
    "/ingest/bulk",
    summary="Bulk Ingest Component Data",
    description="Ingests a batch of component updates. This is the preferred method for efficiency.",
    tags=["Ingestion"]
)
async def ingest_bulk_components(payload: BulkIngestRequest = Body(...)):
    """
    Accepts a list of component updates and processes them. The backend handles
    schema detection, versioning, and buffering.
    """
    processed_count = 0
    errors = []
    for update in payload.updates:
        try:
            # --- Replace with call to your actual service ---
            # store_service.add_component(
            #     entity_id=update.entity_id,
            #     component_type_name=update.component_type,
            #     data=update.data,
            #     step=update.step
            # )
            print(f"Stub: Processing Entity {update.entity_id}, Type {update.component_type}, Step {update.step}")
            processed_count += 1
            # --- End Replace ---
        except Exception as e:
            # Basic error logging; enhance as needed
            errors.append({"entity_id": update.entity_id, "error": str(e)})
            print(f"Error processing update for entity {update.entity_id}: {e}")

    if errors:
        # You might choose to partially succeed or fail the whole batch
        raise HTTPException(
            status_code=400,
            detail={"message": "Errors occurred during bulk ingestion.", "errors": errors}
        )

    # Optionally trigger commit based on buffer size/time internally in the service
    # Or rely on the explicit /commit endpoint

    return {"message": f"Successfully processed {processed_count} component updates."}

@app.post(
    "/commit",
    summary="Trigger Data Commit",
    description="Explicitly triggers the commit of any buffered component data to persistent storage (LanceDB) and updates the catalog.",
    tags=["Ingestion"]
)
async def trigger_commit():
    """
    Forces the backend service to write buffered data to the underlying
    versioned storage.
    """
    try:
        # --- Replace with call to your actual service ---
        # commit_id = store_service.commit()
        commit_id = "dummy_commit_abc789" # Placeholder
        print(f"Stub: Committing data, Commit ID: {commit_id}")
        # --- End Replace ---
        return {"message": "Commit successful.", "commit_id": commit_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Commit failed: {str(e)}")

# --- Optional: Single Ingestion Endpoint ---

@app.post(
    "/ingest/single",
    summary="Single Ingest Component Data",
    description="Ingests a single component update. Less efficient than bulk.",
    tags=["Ingestion"],
    deprecated=True # Often good to guide users towards bulk
)
async def ingest_single_component(payload: ComponentUpdate = Body(...)):
    """
    Accepts and processes a single component update.
    """
    try:
        # --- Replace with call to your actual service ---
        # store_service.add_component(
        #     entity_id=payload.entity_id,
        #     component_type_name=payload.component_type,
        #     data=payload.data,
        #     step=payload.step
        # )
        print(f"Stub: Processing Entity {payload.entity_id}, Type {payload.component_type}, Step {payload.step}")
        # --- End Replace ---

        # Optionally trigger commit here as well, though less common for single adds
        return {"message": "Component update processed successfully."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Example of how to run (if this were the main script)
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)