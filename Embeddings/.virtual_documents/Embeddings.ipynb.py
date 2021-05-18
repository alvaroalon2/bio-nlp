import h5py
import csv 
import numpy
import os
import torch





os.environ['MAX_LENGTH'] = '384'
os.environ['DATA_PATH'] = 'embeddings_test.txt'
os.environ['OUTPUT_PATH'] = 'embeddings_test.h5'
os.environ['BATCH_SIZE'] = '64'
os.environ['MKL_SERVICE_FORCE_INTEL']='1'






# If there's a GPU available...
if torch.cuda.is_available():    

    # Tell PyTorch to use the GPU.    
    device = torch.device("cuda")

    print('There are get_ipython().run_line_magic("d", " GPU(s) available.' % torch.cuda.device_count())")

    print('We will use the GPU:', torch.cuda.get_device_name(0))

# If not...
else:
    print('No GPU available, using the CPU instead.')
    device = torch.device("cpu")


get_ipython().getoutput("python run_embedding.py \")
    --model_name_or_path dmis-lab/biobert-base-cased-v1.1 \
    --max_seq_length  ${MAX_LENGTH} \
    --data_path ${DATA_PATH} \
    --output_path ${OUTPUT_PATH} \
    --batch_size ${BATCH_SIZE} \
    --pooling mean


embeddings = []

with h5py.File(os.environ['OUTPUT_PATH'], 'r') as f:
        with open(os.environ['DATA_PATH'], 'r') as f_in:
            
            print("The number of keys in h5: {}".format(len(f)))
            for i, input in enumerate(f_in):
                entity_name = input.strip()
                
                embedding = f[entity_name]['embedding'][:]
                         
                embeddings += [embedding]

tensor_name = os.environ['DATA_PATH'].split('.')[0]+".tsv"
numpy.savetxt(tensor_name, embeddings, delimiter="\t")



from tensorboard.plugins import projector
get_ipython().run_line_magic("load_ext", " tensorboard")


log_dir='./'
config = projector.ProjectorConfig()
# One can add multiple embeddings.
embedding = config.embeddings.add()
embedding.tensor_name = tensor_name.split('.')[0]
# Link this tensor to its metadata file (e.g. labels).
embedding.tensor_path = tensor_name
embedding.metadata_path = os.environ['DATA_PATH']
projector.visualize_embeddings(log_dir, config)


get_ipython().run_line_magic("tensorboard", " --logdir .")



