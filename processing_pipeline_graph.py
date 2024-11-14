from graphviz import Digraph

# Initialize the directed graph
dot = Digraph(comment='Data Preparation Pipeline')

# Define nodes for data sources
dot.node('Groupage', 'Groupage Data', shape='folder')
dot.node('Innight', 'Innight Data', shape='folder')
dot.node('Pallet', 'Pallet Data', shape='folder')
dot.node('RoadFreight', 'Road Freight Data', shape='folder')
dot.node('Customer', 'Customer Data', shape='folder')
dot.node('Coordinates', 'Coordinates Data', shape='folder')

# Define nodes for processing steps
dot.node('Load', 'Load Main Datasets', shape='box')
dot.node('Combine', 'Combine Datasets', shape='box')
dot.node('Preprocess', 'Preprocess Data', shape='box')
dot.node('MergeCustomer', 'Merge with Customer Data', shape='box')
dot.node('MergeCoords', 'Merge with Cooridinates Data', shape='box')
dot.node('Save', 'Save Processed Data', shape='box')

# Define edges from data sources to loading step
dot.edge('Groupage', 'Load')
dot.edge('Innight', 'Load')
dot.edge('Pallet', 'Load')
dot.edge('RoadFreight', 'Load')
dot.edge('Customer', 'MergeCustomer')
dot.edge('Coordinates', 'MergeCoords')

# Define edges between processing steps
dot.edge('Load', 'Combine')
dot.edge('Combine', 'Preprocess')
dot.edge('Preprocess', 'MergeCustomer')
dot.edge('MergeCustomer', 'MergeCoords')
dot.edge('MergeCoords', 'Save')

# Add a label for the entire graph
dot.attr(label='Data Preparation Pipeline Diagram', labelloc='t', fontsize='20')

# Render and view the graph
dot.render('data_pipeline_diagram.gv', view=True, format='png')
