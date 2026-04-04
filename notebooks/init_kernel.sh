#!/bin/bash
set -e

KERNEL_NAME="tuzhba_kernel"
DISPLAY_NAME="tuzhba_kernel"

conda create -y -n $KERNEL_NAME python=3.11

echo "==> Устанавливаем Java в окружение"
conda install -y -n $KERNEL_NAME -c conda-forge openjdk=17

echo "==> Устанавливаем зависимости"
conda run -n $KERNEL_NAME pip install -r /home/jovyan/work/requirements.txt

echo "==> Регистрируем ядро"
conda run -n $KERNEL_NAME python -m ipykernel install \
    --user \
    --name $KERNEL_NAME \
    --display-name "$DISPLAY_NAME"

echo "==> Готово!"