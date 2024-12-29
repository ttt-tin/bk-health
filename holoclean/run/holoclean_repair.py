import os
import sys

# Thêm đường dẫn cho HoloClean
sys.path.append('./holoclean/')
import holoclean
from detect import NullDetector, ViolationDetector
from repair.featurize import *

data_folder = './holoclean/data'

# Kiểm tra folder tồn tại
if not os.path.exists(data_folder):
    print(f"Error: Data folder '{data_folder}' does not exist.")
    sys.exit(1)

# Lặp qua các file trong folder
for file_name in os.listdir(data_folder):
    if file_name.endswith('_data.csv'):
        # Xác định tên file dữ liệu và constraints
        base_name = file_name.replace('_data.csv', '')
        data_file = os.path.join(data_folder, f'{base_name}_data.csv')
        constraint_file = os.path.join(data_folder, f'{base_name}_constraints.txt')
        
        # Kiểm tra file constraints tồn tại
        if not os.path.exists(constraint_file):
            print(f"Warning: Constraint file for '{base_name}' not found. Skipping.")
            continue
        
        print(f"Processing dataset: {base_name}")
        
        # 1. Setup a HoloClean session
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

        # 2. Load training data and denial constraints
        hc.load_data(base_name, data_file)
        hc.load_dcs(constraint_file)
        hc.ds.set_constraints(hc.get_dcs())

        # 3. Detect erroneous cells using these two detectors
        detectors = [NullDetector(), ViolationDetector()]
        hc.detect_errors(detectors)

        # 4. Repair errors utilizing the defined features
        hc.setup_domain()
        featurizers = [
            InitAttrFeaturizer(),
            OccurAttrFeaturizer(),
            FreqFeaturizer(),
            ConstraintFeaturizer(),
        ]
        hc.repair_errors(featurizers)

        # 5. Evaluate the correctness of the results (optional)
        # hc.evaluate(fpath='../testdata/inf_values_dom.csv',
        #             tid_col='tid',
        #             attr_col='attribute',
        #             val_col='correct_val')

        print(f"Finished processing dataset: {base_name}")
