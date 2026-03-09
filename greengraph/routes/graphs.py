from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from greengraph.database import get_session
from greengraph.models import Graph, GraphCreate, GraphRead, GraphUpdate

router = APIRouter(prefix="/api/graphs", tags=["graphs"])


@router.get("/", response_model=list[GraphRead])
def list_graphs(session: Session = Depends(get_session)):
    return session.exec(select(Graph)).all()


@router.post("/", response_model=GraphRead, status_code=201)
def create_graph(graph_in: GraphCreate, session: Session = Depends(get_session)):
    graph = Graph.model_validate(graph_in)
    session.add(graph)
    session.commit()
    session.refresh(graph)
    return graph


@router.get("/{graph_id}", response_model=GraphRead)
def get_graph(graph_id: int, session: Session = Depends(get_session)):
    graph = session.get(Graph, graph_id)
    if not graph:
        raise HTTPException(status_code=404, detail="Graph not found")
    return graph


@router.put("/{graph_id}", response_model=GraphRead)
def update_graph(graph_id: int, graph_in: GraphUpdate, session: Session = Depends(get_session)):
    graph = session.get(Graph, graph_id)
    if not graph:
        raise HTTPException(status_code=404, detail="Graph not found")

    update_data = graph_in.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(graph, key, value)
    graph.updated_at = datetime.utcnow()

    session.add(graph)
    session.commit()
    session.refresh(graph)
    return graph


@router.delete("/{graph_id}", status_code=204)
def delete_graph(graph_id: int, session: Session = Depends(get_session)):
    graph = session.get(Graph, graph_id)
    if not graph:
        raise HTTPException(status_code=404, detail="Graph not found")
    session.delete(graph)
    session.commit()
