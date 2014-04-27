// generate identity matrix
function eye(dim) {
	var identity = linalg.newMat({"rows":dim, "cols":dim});
	for (var rowN = 0; rowN < identity.rows; rowN++) {
		identity.put(rowN, rowN, 1.0);
	}
	return identity;
}

function copyFltArrayToVec(arr) {
    var len = arr.length;
    var vec = linalg.newVec({ "vals": arr.length });
    for (var elN = 0; elN < len; elN++) {
        vec[elN] = arr[elN];
    }
    return vec;
}

//////////// RIDGE REGRESSION 
// solve a regularized least squares problem
// Input parameters: regularization factor, dimension, buffer
exports.ridgeRegression = function(kapa, dim, buffer) {
    var X = [];
    var y = [];
    buffer = typeof buffer !== 'undefined' ? buffer : -1;
    var w = linalg.newVec({ "vals": dim });

    this.add = function(x, target) {
        X.push(x);
        y.push(target);
        if (buffer > 0) {
            if (X.length > buffer) {
                this.forget(X.length - buffer);
            }
        }
    };
    this.addupdate = function (x, target) {
        this.add(x, target);
        this.update();
    };

    this.forget = function (ndeleted) {
        ndeleted = typeof ndeleted !== 'undefined' ? ndeleted : 1;
        ndeleted = Math.min(X.length, ndeleted);
        X.splice(0, ndeleted);
        y.splice(0, ndeleted);
    };
    this.update = function () {
        var A = this.getMatrix();
        var b = copyFltArrayToVec(y);
        w = this.compute(A,b);
    };
    this.getModel = function() {
        return w;
    };
    this.getMatrix = function () {
        if (X.length > 0) {
            var A = linalg.newMat({ "cols": X[0].length, "rows": X.length });
            for (var i = 0; i < X.length; i++) {
                A.setRow(i, X[i]);
            }
            return A;
        }
    };
    this.compute = function (A, b) {
        var I = eye(A.cols);
        var coefs = (A.transpose().multiply(A).plus(I.multiply(kapa))).solve(A.transpose().multiply(b));
        return coefs;
    };

    this.predict = function (x) {
        return w.inner(x);
    };


};

// About this module
exports.about = function() {
	var description = "Ridge regression module. Input parameters: regularization factor, dimension, buffer";
	return description;
};