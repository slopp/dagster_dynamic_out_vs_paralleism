from dagster import op, DynamicOut, job, DynamicOutput, OpExecutionContext, Definitions
import time
from typing import List

@op(out=DynamicOut())
def load_pieces(context: OpExecutionContext):
    pieces_to_process =  [chr(i) for i in range(ord('a'), ord('z')+1)] # list a-z
    context.log.info(f"Will process... {pieces_to_process}")

    # creates an output per letter, chunking is also possible
    for piece in pieces_to_process: 
        yield DynamicOutput(piece, mapping_key=piece)

@op
def compute_piece(piece_to_compute: str):
    time.sleep(1)
    return piece_to_compute.upper()

@op
def merge_and_analyze(context: OpExecutionContext, computed_pieces: List[str]):
    context.log.info(f"Finished processing, result is ... {computed_pieces}")
    return

@job
def dynamic_graph():
    pieces = load_pieces()
    results = pieces.map(compute_piece)
    merge_and_analyze(results.collect())

defs = Definitions(
    jobs=[dynamic_graph]
)