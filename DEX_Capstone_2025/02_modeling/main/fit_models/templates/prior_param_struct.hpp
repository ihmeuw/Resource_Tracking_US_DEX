// how to read in an object that is used as a prior for standard devs of res
template<class Type>
  struct prior_type_sigma {
    std::string name;
    Type par1;
    Type par2;
    
    prior_type_sigma(SEXP x){
      name = CHAR(STRING_ELT(getListElement(x,"type"), 0));
      par1 = asVector<float>(getListElement(x,"par1"))[0];
      par2 = asVector<float>(getListElement(x,"par2"))[0];
    }
  };