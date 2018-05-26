import numpy as np
import math
import sys
import time
#start=time.time()

#n=100
#m=4382
#f=40
#k=10
#f_in=open('ratings_task1.csv','r')

#Initialization
f_in= sys.argv[1]
n = int(sys.argv[2])
m = int(sys.argv[3])
f = int(sys.argv[4])
k = int(sys.argv[5])

#Building Utility Matrix
ratings=np.loadtxt(f_in,skiprows=1,delimiter=',')[:,:3]
rows, row_ = np.unique(ratings[:, 0], return_inverse=True)
cols, col_ = np.unique(ratings[:, 1], return_inverse=True)

utility_matrix = np.zeros((len(rows), len(cols)), dtype=ratings.dtype)
utility_matrix[:]=np.NAN
utility_matrix[row_, col_] = ratings[:, 2]

#Initializing M, U and V matrices
M_matrix=utility_matrix[:n,:m]
U_matrix=np.ones((n,f))
V_matrix=np.ones((f,m))

#count of non-zero elements in M matrix
nze=np.count_nonzero(~np.isnan(M_matrix))

#print utility_matrix.shape
#print M_matrix.shape
#print U_matrix.shape
#print V_matrix.shape
#print k

for iterations in range(k):
    #updation of each element value in U matrix row-wise
    for Urow in range(n):
        for Ucol in range(f):
            u_r=np.dot(U_matrix[Urow],V_matrix)
            ur=U_matrix[Urow][Ucol]*V_matrix[Ucol]
            u_r=u_r-ur
            diff=M_matrix[Urow]-u_r
            diff=diff*V_matrix[Ucol]
            mr=V_matrix[Ucol]+diff-diff
            x=np.nansum(diff)/np.nansum(np.square(mr))
            U_matrix[Urow][Ucol]=x
    #updation of each element value in V matrix column-wise
    for Vcol in range(m):
        for Vrow in range(f):
            v_c=np.dot(U_matrix,V_matrix[:,Vcol])
            vc=V_matrix[Vrow][Vcol]*U_matrix[:,Vrow]
            v_c=v_c-vc
            diff=M_matrix[:,Vcol]-v_c
            diff=diff*U_matrix[:,Vrow]
            mr=U_matrix[:,Vrow]+diff-diff
            y=np.nansum(diff)/np.nansum(np.square(mr))
            V_matrix[Vrow][Vcol]=y

    Mprime_matrix=np.dot(U_matrix,V_matrix)
    err=np.square(abs(Mprime_matrix-M_matrix))
    terr=np.nansum(err)
    rmse=math.sqrt(terr/nze)
    print "%.4f"%rmse
#print U_matrix
#print V_matrix
#print M_matrix
#print Mprime_matrix
#print "Time taken :"+str((time.time()-start))
