import json
from sklearn.model_selection import train_test_split
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F

def load_data(file_path: str) -> torch.Tensor:
    with open(file_path, 'r') as file:
        data = json.load(file)

    X, y = zip(*data)
    X_train, X_test, y_train, y_test = train_test_split(
        np.array(X), np.array(y), test_size=0.2, random_state=42
    )

    return X_train, X_test, y_train, y_test

def encode_all(X_train, X_test):
    """
    Simplified Encoding
    Map each unique value to a unique integer (skip 0 for unknown)
    """
    song_id_to_index = {}
    index_to_song_id = {}

    for i, song_id in enumerate(X_train):
        song_id_to_index[song_id] = i + 1
        index_to_song_id[i + 1] = song_id

    for i, song_id in enumerate(X_test):
        if song_id not in song_id_to_index:
            song_id_to_index[song_id] = len(song_id_to_index) + 1
            index_to_song_id[len(index_to_song_id) + 1] = song_id

    # Add 0 for unknown
    song_id_to_index[0] = 0
    index_to_song_id[0] = 0

    return song_id_to_index, index_to_song_id


def train_epoch(model, X_train, y_train, optimizer):
    """
    Function for training a single epoch, validation still needs to be implemented
    """
    model.train()
    yHat = model(X_train)
    preds = F.softmax(yHat, dim=1)
    loss = F.cross_entropy(preds, y_train)
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()
    return loss


def get_model(input_size:int, output_size:int) -> nn.Sequential:
    """
    Function for creating a model
    """
    model = nn.Sequential(
        nn.Embedding(input_size, 100),
        nn.Linear(100, output_size),
    )
    return model

def train_model(file_path, num_epochs:int) -> None:
    """
    Function for training the model
    """
    X_train, X_test, y_train, y_test = load_data(file_path)
    song_id_to_index, index_to_song_id = encode_all(X_train, X_test)

    X_train = [song_id_to_index[song_id] for song_id in X_train]
    X_test = [song_id_to_index.get(song_id, 0) for song_id in X_test]
    y_train = [song_id_to_index.get(song_id, 0) for song_id in y_train]
    y_test = [song_id_to_index.get(song_id, 0) for song_id in y_test]

    X_train = torch.tensor(X_train)
    X_test = torch.tensor(X_test)
    y_train = torch.tensor(y_train)
    y_test = torch.tensor(y_test)

    X_train = F.one_hot(X_train, len(song_id_to_index))
    X_test = F.one_hot(X_test, len(song_id_to_index))
    y_train = F.one_hot(y_train, len(song_id_to_index))
    y_test = F.one_hot(y_test, len(song_id_to_index))

    model = get_model(len(song_id_to_index), len(song_id_to_index))
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

    for epoch in range(num_epochs):
        loss = train_epoch(model, X_train, y_train, optimizer)
        print(f"Epoch {epoch} loss: {loss:.4f}")

    torch.save(model[0].weight, 'embeddings.pt')
    print("Saved embeddings to embeddings.pt")

    return index_to_song_id

if __name__ == "__main__":
    train_flow(5)
