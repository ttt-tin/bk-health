import sys
sys.path.append('../')
import holoclean
from detect import ErrorsLoaderDetector
from repair.featurize import *
import os

hc = holoclean.HoloClean(
    db_name='holo',
    domain_thresh_1=0,
    domain_thresh_2=0,
    weak_label_thresh=0.99,
    max_domain=10000,
    cor_strength=0.6,
    nb_cor_strength=0.8,
    epochs=10,
    weight_decay=0.01,
    learning_rate=0.001,
    threads=1,
    batch_size=1,
    verbose=True,
    timeout=3*60000,
    feature_norm=False,
    weight_norm=False,
    print_fw=True
).session


def clean_data(schema_name, file_path, constraint_path):
    hc.load_data(schema_name, file_path)
    hc.load_dcs(constraint_path)
    hc.ds.set_constraints(hc.get_dcs())

    error_loader = ErrorsLoaderDetector(
        db_engine=hc.ds.engine,
        schema_name=schema_name,
        table_name='dk_cells'
    )
    hc.detect_errors([error_loader])

    hc.setup_domain()
    featurizers = [
        OccurAttrFeaturizer(),
        FreqFeaturizer(),
        ConstraintFeaturizer(),
    ]

    hc.repair_errors(featurizers)

    hc.evaluate(fpath=f'../cleaned/{schema_name}_clean.csv',
                tid_col='tid',
                attr_col='attribute',
                val_col='correct_val')
    

def clean_data(schema_name, file_path, constraint_path):
    hc.load_data(schema_name, file_path)
    hc.load_dcs(constraint_path)
    hc.ds.set_constraints(hc.get_dcs())

    error_loader = ErrorsLoaderDetector(
        db_engine=hc.ds.engine,
        schema_name=schema_name,
        table_name='dk_cells'
    )
    hc.detect_errors([error_loader])

    hc.setup_domain()
    featurizers = [
        OccurAttrFeaturizer(),
        FreqFeaturizer(),
        ConstraintFeaturizer(),
    ]

    hc.repair_errors(featurizers)

    hc.evaluate(fpath=f'../cleaned/{schema_name}_clean.csv',
                tid_col='tid',
                attr_col='attribute',
                val_col='correct_val')
    

def clean_data(schema_name, file_path, constraint_path):
    hc.load_data(schema_name, file_path)
    hc.load_dcs(constraint_path)
    hc.ds.set_constraints(hc.get_dcs())

    error_loader = ErrorsLoaderDetector(
        db_engine=hc.ds.engine,
        schema_name=schema_name,
        table_name='dk_cells'
    )
    hc.detect_errors([error_loader])

    hc.setup_domain()
    featurizers = [
        OccurAttrFeaturizer(),
        FreqFeaturizer(),
        ConstraintFeaturizer(),
    ]

    hc.repair_errors(featurizers)

    hc.evaluate(fpath=f'../cleaned/{schema_name}_clean.csv',
                tid_col='tid',
                attr_col='attribute',
                val_col='correct_val')
    

def main():
    data_folder = './data'
    constraint_folder = './constraint'

    data_files = [f for f in os.listdir(data_folder) if f.endswith('.csv')]

    for data_file in data_files:
        schema_name = os.path.splitext(data_file)[0]
        data_path = os.path.join(data_folder, data_file)
        constraint_file = f"{schema_name}_constraint.dc"
        constraint_path = os.path.join(constraint_folder, constraint_file)

        if not os.path.exists(constraint_path):
            print(f"Constraint file not found for schema: {schema_name}, skipping...")
            continue

        print(f"Processing schema: {schema_name}")
        clean_data(schema_name, data_path, constraint_path)

if __name__ == "__main__":
    main()