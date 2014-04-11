There are various perspectives relating to 'length' in the LaTiS data model.
++Should we overload the meaning of 'length' or use different names for the different notions:

* getTextLength:   Number of characters in a Text value
* getElementCount: Number of elements in a Tuple
* getSampleCount:  Number of samples in a Function
* getLength:       Total number of samples in a Dataset with nested Functions

Note, 'size' has similar issues, but it is used in LaTiS to represent the number of bytes of data.

The length of a Function should be the number of samples represented by its domain set.
It is of no consequence if the range contains other (nested) Functions. Consider the Functions:
  f1: (x,y) -> a
  f2: x -> (y -> a)
These could represent the same set of data but f1 has a 2D domain so the number of
samples is nx*ny (assuming the 2D domain is not an irregular set), while the number 
of samples in f2 is just nx. The 'total length' of each Dataset encapsulating either of these 
would be the same (assuming ny is not a function of x).

Dataset with multiple top level variables:
ds: (a, (b,c,d), x->e, y->z->f)
length = 1 + 3 + nx + ny*nz
If nz is a function of y, then the length from it would be the integral of nz(y) from y=1,ny.
++need word for nested Function with inner domain set that is not a function of the outer
  visad used FlatField for similarly simple data
  *cartesian? think product set
  http://en.wikipedia.org/wiki/Cartesian_closed_category
  http://en.wikipedia.org/wiki/Cartesian_diagram: pullback ~ fiber bundle!
    Equijoin in relational algebra.
