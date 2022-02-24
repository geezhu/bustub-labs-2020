//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <iterator>
#include <memory>
#include <utility>

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // DONE(P0): Add implementation
  Matrix(int r, int c) {
    linear = new T[r * c];
    rows = r;
    cols = c;
  }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // DONE(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // DONE(P0): Add implementation
  virtual ~Matrix() { delete[] linear; }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // DONE(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    data_ = new T *[r];
    auto linear1 = this->linear;
    for (int i = 0; i < r; ++i) {
      data_[i] = linear1;
      linear1 += c;
    }
  }

  // DONE(P0): Add implementation
  int GetRows() override { return this->rows; }

  // DONE(P0): Add implementation
  int GetColumns() override { return this->cols; }

  // DONE(P0): Add implementation
  T GetElem(int i, int j) override { return data_[i][j]; }

  // DONE(P0): Add implementation
  void SetElem(int i, int j, T val) override { data_[i][j] = val; }

  // DONE(P0): Add implementation
  void MatImport(T *arr) override {
    if (arr == nullptr || sizeof(arr) != sizeof(this->linear)) {
      return;
    }
    for (int i = 0; i < this->rows * this->cols; ++i) {
      this->linear[i] = arr[i];
    }
  }

  // DONE(P0): Add implementation
  ~RowMatrix() override { delete[] data_; };

 private:
  // 2D array containing the elements of the matrix in row-major format
  // DONE(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> &&mat1,
                                                   std::unique_ptr<RowMatrix<T>> &&mat2) {
    // DONE(P0): Add code
    int r = 0;
    int c = 0;
    if (!mat1 || !mat2 || (r = mat1->GetRows()) != mat2->GetRows() || (c = mat1->GetColumns()) != mat2->GetColumns()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    //    auto mat3=std::make_unique<RowMatrix<T>>(r,c);
    auto mat3 = std::unique_ptr<RowMatrix<T>>(new RowMatrix<T>(r, c));
    for (int i = 0; i < r; ++i) {
      for (int j = 0; j < c; ++j) {
        mat3->SetElem(i, j, mat1->GetElem(i, j) + mat2->GetElem(i, j));
      }
    }
    return mat3;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> &&mat1,
                                                        std::unique_ptr<RowMatrix<T>> &&mat2) {
    // DONE(P0): Add code

    if (!mat1 || !mat2 || (mat1->GetColumns()) != mat2->GetRows()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    int c = mat2->GetColumns();
    int r = mat1->GetRows();
    int r2c1 = mat1->GetColumns();
    auto multiplyLine = [&mat1, &mat2](int r1, int c2, int r2c1) -> T {
      T count = 0;
      for (int i = 0; i < r2c1; ++i) {
        count += mat1->GetElem(r1, i) * mat2->GetElem(i, c2);
      }
      return count;
    };
    //    auto mat3=std::make_unique<RowMatrix<T>>(r,c);
    auto mat3 = std::unique_ptr<RowMatrix<T>>(new RowMatrix<T>(r, c));
    for (int i = 0; i < r; ++i) {
      for (int j = 0; j < c; ++j) {
        mat3->SetElem(i, j, multiplyLine(i, j, r2c1));
      }
    }
    return mat3;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> &&matA,
                                                    std::unique_ptr<RowMatrix<T>> &&matB,
                                                    std::unique_ptr<RowMatrix<T>> &&matC) {
    // DONE(P0): Add code
    return AddMatrices(std::move(MultiplyMatrices(std::move(matA), std::move(matB))), std::move(matC));
  }
};
}  // namespace bustub
