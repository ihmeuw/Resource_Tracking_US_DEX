// Simple model, with LCAR on location (without year)
// 
// Authors: Azalea Thomson and Haley Lescinsky

#include <TMB.hpp>
#include </FILEPATH/mkl.h>
using namespace density;
using Eigen::SparseMatrix; 
#include "lcar_strmat.hpp"
#include "prior_param_struct.hpp"
#include "prior_penalty.hpp"

// Objective function
template<class Type>
Type objective_function<Type>::operator() () {
  
  printf("MKL threads (mkl_get_max_threads): %d\n", mkl_get_max_threads());
  
  // Define data and inputs - these already exist (the inputs)
  // --------------------------------------------------------------
  DATA_STRING(link);  // link function - "poisson" or "gaussian"
  DATA_VECTOR(Y);     // value
  DATA_VECTOR(N);     // n_obs
  DATA_VECTOR(SE);    // standard error of the data point
  DATA_MATRIX(X);     // covariates (at minimum, a column for the intercept) 
  DATA_IVECTOR(A);    // age indicator
  DATA_IVECTOR(J);    // area indicator
  DATA_IVECTOR(T);    // year indicator
  DATA_IVECTOR(D);    // data source indicator

  DATA_SPARSE_MATRIX(graph_a);
  DATA_SPARSE_MATRIX(graph_j);  // neighborhood structure
  //DATA_SPARSE_MATRIX(graph_t); 
  
  DATA_INTEGER(num_d);
  DATA_INTEGER(rho_t_alpha); // = 20
  DATA_INTEGER(rho_t_beta); // = 2
  DATA_INTEGER(rho_a_alpha); // = 20
  DATA_INTEGER(rho_a_beta); // = 2
  DATA_INTEGER(rho_j_alpha); // = 20
  DATA_INTEGER(rho_j_beta); // = 2
  DATA_INTEGER(sigma_alpha); //= 1
  DATA_INTEGER(sigma_beta); //= 4
  
  // Define parameters
  // --------------------------------------------------------------
  Type nll = 0; //start with nll of zero, then all effects get added to this 
  
  // fixed effects (intercept, covariate effects)
  PARAMETER_VECTOR(B);
  
  // RE1: LCAR(age):IID(dataset)
  PARAMETER(re1_log_sigma); 
  Type sigma_1 = exp(re1_log_sigma); 
  PARAMETER(logit_rho_1a);  
  Type rho_1a = invlogit(logit_rho_1a); 
  nll -= dgamma(sigma_1, Type(sigma_alpha), Type(sigma_beta), true);
  nll -= dbeta(rho_1a, Type(rho_a_alpha), Type(rho_a_beta), true);
  
  // RE2: LCAR(location)
  PARAMETER(re2_log_sigma); 
  Type sigma_2 = exp(re2_log_sigma); 
  PARAMETER(logit_rho_2);  
  Type rho_2 = invlogit(logit_rho_2);
  nll -= dgamma(sigma_2, Type(sigma_alpha), Type(sigma_beta), true);
  nll -= dbeta(rho_2, Type(rho_j_alpha), Type(rho_j_beta), true);
  
  
  // // RE3: LCAR(time):IID(dataset)
  // PARAMETER(re3_log_sigma); 
  // Type sigma_3 = exp(re3_log_sigma); 
  // PARAMETER(logit_rho_3t);  
  // Type rho_3t = invlogit(logit_rho_3t); 
  // nll -= dgamma(sigma_3, Type(sigma_alpha), Type(sigma_beta), true);
  // nll -= dbeta(rho_3t, Type(rho_t_alpha), Type(rho_t_beta), true);

  PARAMETER_ARRAY(re1);  
  PARAMETER_VECTOR(re2);  
  //PARAMETER_ARRAY(re3); 
  
  // Evaluate random effects, and add to NLL
  // --------------------------------------------------------------
  
  
  // RE1: LCAR(age):IID(dataset):LCAR(location) 
  SparseMatrix<Type> K_1a = lcar_strmat(graph_a, rho_1a);  
  matrix<Type> Sigma_d(num_d, num_d);
  Sigma_d.setIdentity();
  Eigen::SparseMatrix<Type> Sigma_Sparse_d = asSparseMatrix(Sigma_d);
  REPORT(Sigma_Sparse_d);
  nll += SCALE(SEPARABLE(GMRF(Sigma_Sparse_d), GMRF(K_1a)), sigma_1)(re1); 
  
  // RE2: LCAR(location)
  SparseMatrix<Type> K_2 = lcar_strmat(graph_j, rho_2);  
  nll += SCALE(GMRF(K_2), sigma_2)(re2); 
  
  // // RE3: LCAR(time):LCAR(location)
  // SparseMatrix<Type> K_3t = lcar_strmat(graph_t, rho_3t);  
  // nll += SCALE(SEPARABLE(GMRF(Sigma_Sparse_d), GMRF(K_3t)), sigma_3)(re3);

  // Add to nll
  nll -= dnorm(logit_rho_1a, Type(0), Type(1.5), true);
  nll -= dnorm(logit_rho_2, Type(0), Type(1.5), true);
  //nll -= dnorm(logit_rho_3t, Type(0), Type(1.5), true);

  // Evaluate data effects, and add to NLL
  // --------------------------------------------------------------
  
  // given data, how likely are our parameters
  // for each data point, construct our prediction and calculate the corresponding conditional probability of the data point using the chosen (here poisson) distribution
  
  if (link == "poisson") {
    // construct predictions of log m
    vector<Type> log_m = X * B; 
    for(size_t i = 0; i < Y.size(); i++)
      log_m[i] += re1(A[i], D[i])  + re2[J[i]] ;
      vector<Type> m = exp(log_m); 
    
    // get corresponding conditional likelihood
    for(size_t i = 0; i < Y.size(); i++)
      nll -= dpois(Y[i], N[i]*m[i], true); // for poisson distribution
  
  }else{
    // construct predictions of log mu
    vector<Type> log_mu = X * B;
    for(size_t i = 0; i < Y.size(); i++)
      log_mu[i] += re1(A[i], D[i])  + re2[J[i]] ;
      vector<Type> mu = exp(log_mu); 
    
    // data likelihood
    for(size_t i = 0; i < Y.size(); i++) 
      nll -= dnorm(Y[i], mu[i], SE[i], true);
    
  }
  
  return nll;
}