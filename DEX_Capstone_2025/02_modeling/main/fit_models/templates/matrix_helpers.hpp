// ///////////////////////////////////////////////////////////////////////////////////////
  //
  // WORKING WITH PRECISION MATRICES
//
  // AUTHOR: Nathaniel Henry
// CREATED: 28 June 2022
// PURPOSE: Helper functions for working with precision matrices
//
  // ///////////////////////////////////////////////////////////////////////////////////////
  
  using Eigen::SparseMatrix;


// Function for rescaling a precision matrix to have standard deviation sigma
//
  // Parameter Q: Unscaled precision matrix
// Parameter sigma: Standard deviation to scale to
//
  template<class Type>
  SparseMatrix<Type> scale_precision(SparseMatrix<Type> Q, Type sigma){
    SparseMatrix<Type> Q_scaled = Q / (sigma * sigma);
    return Q_scaled;
  }


// Function to create an IID precision matrix (AKA a scaled identity matrix)
//
  // Parameter dim: Number of rows (and columns) for the precision matrix
// Parameter sigma: Standard deviation of the iid process
//
  template<class Type>
  SparseMatrix<Type> iid_precision(int dim, Type sigma = 1.0){
    SparseMatrix<Type> I(dim, dim);
    for(int ii=0; ii < dim; ii++){
      I.insert(ii, ii) = 1.0;
    }
    SparseMatrix<Type> I_scaled = scale_precision(I, sigma);
    return I_scaled;
  }


// Function to create a precision matrix corresponding with an autoregressive process of
//   order 1 (AR1 process). Adapted from the `ar.matrix` package:
  //   https://rdrr.io/cran/ar.matrix/src/R/Q.AR1.R
//
  // Corresponds to the process x_t = rho * x_(t-1) + epsilon
  //
    // Parameter steps: Number of steps (in time, age, etc) for the autoregressive process
  // Paramter rho: Correlation between step t and step t+1
  // Parameter sigma: Standard deviation of the random noise term epsilon
  //
    template<class Type>
    SparseMatrix<Type> ar1_precision(int steps, Type rho, Type sigma = 1.0){
      SparseMatrix<Type> Q(steps, steps);
      for(int ii=1; ii < steps; ii++){
        Q.insert(ii, ii) = 1.0 + rho * rho;
        Q.insert(ii - 1, ii) = rho * -1.0;
        Q.insert(ii, ii - 1) = rho * -1.0;
      }
      Q.insert(0, 0) = 1.0;
      Q.insert(steps, steps) = 1.0;
      SparseMatrix<Type> Q_scaled = scale_precision(Q, sigma);
      return Q_scaled;
    }
  
  
  // Function for preparing a precision matrix corresponding to a correlated autoregressive
  // (CAR) spatial model. In the CAR model, the spatial term for each polygon is conditional
  // on the spatial terms of its neighbors.
  //
    // Besag (1974) showed that the CAR model can be represented as a multivariate normal
  // distribution centered at zero and with variance-covariance matrix SIGMA. The
  // distribution is usually parameterized using the precision matrix Q = SIGMA^-1 because Q
  // tends to be sparse, which is computationally convenient.
  //
    // The precision matrix Q for a CAR model is based on the adjacency matrix, a square
  // matrix with dimensions (# locations) by (# locations), where entry {i, j} is 1 if
    // polygon i is "adjacent to" polygon j and 0 otherwise. Adjacency is usually defined by
    // two polygons bordering each other, but can be tweaked to e.g. define neighborhood
    // relationships between islands.
    //
      // The precision matrix Q can be written as:
      //   tau * (D - rho * W)
    // where:
      //   -> tau = The precision scaling parameter (a scalar)
    //   -> W = the adjacency matrix ('W' for adjacency 'weights')
    //   -> D = a diagonal matrix where each diagonal term {i,i} is the row sum of W_{i,}
    //          ('D' for 'diagonal')
    //   -> rho = An autocorrelation parameter. As rho => 1, the joint distribution approaches
    //      perfect spatial autocorrelation. As rho => 0, the joint distribution approaches an
    //      IID distribution (that is, no autocorrelation effect)
    //
      // In this distribution, we leave out the precision scaling parameter tau because it will
    //   typically be applied using the TMB SCALE() function.
    //
      // For more, see:
      // Original paper: Besag, J. (1974). Spatial interaction and the statistical analysis of
    // lattice systems. Journal of the Royal Statistical Society: Series B (Methodological),
    // 36(2), 192-225. https://www.jstor.org/stable/pdf/2984812.pdf
    //
      // Tutorial on varieties of CAR models: https://bit.ly/3dV8qc4
    //
      // Parameter W: sparse adjacency matrix
    // Parameter rho: spatial autocorrelation term
    template<class Type>
      SparseMatrix<Type> car_precision(SparseMatrix<Type> W, Type rho = 0.5){
        int dim = W.rows();
        SparseMatrix<Type> D(dim, dim);
        for(int ii=0; ii < dim; ii++){
          D.insert(ii, ii) = W.row(ii).sum();
        }
        SparseMatrix<Type> Q_car = D - rho * W;
        return Q_car;
      }