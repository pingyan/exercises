Step #0 get tensorflow env setup 
sudo pip install --ignore-installed six --upgrade $TF_BINARY_URL 
```
source ~/tensorflow/bin/activate 
```
Step #1 extract page transitions (bigrams) by sessions from raw data
```
pyspark timeLapse.py input output
```
Step #2 convert time lapse records into 2-D arrays 
```
pyspark matrixize.py input outputTensors
```

Step #3 train the autoencoder and evaluate the anomaly detection capability
```
python autoencoder.py input output
```

