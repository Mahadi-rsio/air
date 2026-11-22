package readadapter

import (
	"fmt"
	"math/big"
	"strings"
)

type pgNumKind uint8

const (
	pgNumFinite pgNumKind = iota
	pgNumNaN
	pgNumPosInf
	pgNumNegInf
)

type pgDecimal struct {
	kind  pgNumKind
	neg   bool
	coeff *big.Int // always >= 0 when finite
	scale int      // >= 0
}

var bigTen = big.NewInt(10)

func pow10(n int) *big.Int {
	if n <= 0 {
		return big.NewInt(1)
	}
	return new(big.Int).Exp(bigTen, big.NewInt(int64(n)), nil)
}

func parsePGDecimal(s string) (pgDecimal, error) {
	s = strings.TrimSpace(s)
	switch s {
	case "NaN", "nan", "NAN":
		return pgDecimal{kind: pgNumNaN}, nil
	case "Infinity", "infinity", "+Infinity", "+infinity", "inf", "+inf", "INF", "+INF":
		return pgDecimal{kind: pgNumPosInf}, nil
	case "-Infinity", "-infinity", "-inf", "-INF":
		return pgDecimal{kind: pgNumNegInf}, nil
	}
	if s == "" {
		return pgDecimal{}, fmt.Errorf("invalid numeric %q", s)
	}
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	} else if s[0] == '+' {
		s = s[1:]
	}
	if s == "" {
		return pgDecimal{}, fmt.Errorf("invalid numeric %q", s)
	}
	var intPart, fracPart string
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart = s[:dot]
		fracPart = s[dot+1:]
	} else {
		intPart = s
	}
	if intPart == "" && fracPart == "" {
		return pgDecimal{}, fmt.Errorf("invalid numeric %q", s)
	}
	digits := intPart + fracPart
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return pgDecimal{}, fmt.Errorf("invalid numeric %q", s)
		}
	}
	coeff := new(big.Int)
	if _, ok := coeff.SetString(digits, 10); !ok {
		return pgDecimal{}, fmt.Errorf("invalid numeric %q", s)
	}
	if coeff.Sign() == 0 {
		neg = false
	}
	return pgDecimal{
		kind:  pgNumFinite,
		neg:   neg,
		coeff: coeff,
		scale: len(fracPart),
	}, nil
}

func formatPGDecimal(d pgDecimal) string {
	switch d.kind {
	case pgNumNaN:
		return "NaN"
	case pgNumPosInf:
		return "Infinity"
	case pgNumNegInf:
		return "-Infinity"
	}
	if d.coeff == nil || d.coeff.Sign() == 0 {
		if d.scale > 0 {
			return "0." + strings.Repeat("0", d.scale)
		}
		return "0"
	}
	digits := d.coeff.String()
	var b strings.Builder
	if d.neg {
		b.WriteByte('-')
	}
	if d.scale == 0 {
		b.WriteString(digits)
		return b.String()
	}
	if len(digits) <= d.scale {
		b.WriteString("0.")
		for i := 0; i < d.scale-len(digits); i++ {
			b.WriteByte('0')
		}
		b.WriteString(digits)
		return b.String()
	}
	split := len(digits) - d.scale
	b.WriteString(digits[:split])
	b.WriteByte('.')
	b.WriteString(digits[split:])
	return b.String()
}

func (d pgDecimal) signedCoeff() *big.Int {
	if d.neg {
		return new(big.Int).Neg(d.coeff)
	}
	return new(big.Int).Set(d.coeff)
}

func pgDecimalAdd(a, b pgDecimal) pgDecimal {
	if a.kind == pgNumNaN || b.kind == pgNumNaN {
		return pgDecimal{kind: pgNumNaN}
	}
	if a.kind != pgNumFinite || b.kind != pgNumFinite {
		if a.kind == pgNumPosInf && b.kind == pgNumNegInf || a.kind == pgNumNegInf && b.kind == pgNumPosInf {
			return pgDecimal{kind: pgNumNaN}
		}
		if a.kind == pgNumPosInf || b.kind == pgNumPosInf {
			return pgDecimal{kind: pgNumPosInf}
		}
		return pgDecimal{kind: pgNumNegInf}
	}
	scale := a.scale
	if b.scale > scale {
		scale = b.scale
	}
	ca := a.signedCoeff()
	if scale > a.scale {
		ca.Mul(ca, pow10(scale-a.scale))
	}
	cb := b.signedCoeff()
	if scale > b.scale {
		cb.Mul(cb, pow10(scale-b.scale))
	}
	sum := ca.Add(ca, cb)
	neg := sum.Sign() < 0
	if neg {
		sum.Neg(sum)
	}
	return pgDecimal{kind: pgNumFinite, neg: neg, coeff: sum, scale: scale}
}

func pgDecimalSub(a, b pgDecimal) pgDecimal {
	switch b.kind {
	case pgNumPosInf:
		b.kind = pgNumNegInf
	case pgNumNegInf:
		b.kind = pgNumPosInf
	case pgNumFinite:
		if b.coeff.Sign() != 0 {
			b.neg = !b.neg
		}
	}
	return pgDecimalAdd(a, b)
}

func pgDecimalMul(a, b pgDecimal) pgDecimal {
	if a.kind == pgNumNaN || b.kind == pgNumNaN {
		return pgDecimal{kind: pgNumNaN}
	}
	if a.kind != pgNumFinite || b.kind != pgNumFinite {
		if (a.kind == pgNumFinite && a.coeff.Sign() == 0) || (b.kind == pgNumFinite && b.coeff.Sign() == 0) {
			return pgDecimal{kind: pgNumNaN}
		}
		aNeg := a.kind == pgNumNegInf || (a.kind == pgNumFinite && a.neg)
		bNeg := b.kind == pgNumNegInf || (b.kind == pgNumFinite && b.neg)
		if aNeg != bNeg {
			return pgDecimal{kind: pgNumNegInf}
		}
		return pgDecimal{kind: pgNumPosInf}
	}
	prod := new(big.Int).Mul(a.coeff, b.coeff)
	neg := (a.neg != b.neg) && prod.Sign() != 0
	return pgDecimal{
		kind:  pgNumFinite,
		neg:   neg,
		coeff: prod,
		scale: a.scale + b.scale,
	}
}

func pgDecimalDiv(a, b pgDecimal, rscale int) (pgDecimal, error) {
	if a.kind == pgNumNaN || b.kind == pgNumNaN {
		return pgDecimal{kind: pgNumNaN}, nil
	}
	if b.kind == pgNumFinite && b.coeff.Sign() == 0 {
		return pgDecimal{}, fmt.Errorf("division by zero")
	}
	if a.kind != pgNumFinite && b.kind != pgNumFinite {
		return pgDecimal{kind: pgNumNaN}, nil
	}
	if a.kind != pgNumFinite {
		aNeg := a.kind == pgNumNegInf
		bNeg := b.neg
		if aNeg != bNeg {
			return pgDecimal{kind: pgNumNegInf}, nil
		}
		return pgDecimal{kind: pgNumPosInf}, nil
	}
	if b.kind != pgNumFinite {
		return pgDecimal{kind: pgNumFinite, coeff: big.NewInt(0), scale: 0}, nil
	}
	if rscale < 0 {
		rscale = 16
	}
	// Compute round( (a * 10^(rscale + b.scale - a.scale)) / b )
	num := new(big.Int).Set(a.coeff)
	den := new(big.Int).Set(b.coeff)
	exp := rscale + b.scale - a.scale
	if exp >= 0 {
		num.Mul(num, pow10(exp))
	} else {
		den.Mul(den, pow10(-exp))
	}
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	// Round half away from zero (PostgreSQL numeric rounding)
	rem2 := new(big.Int).Lsh(rem, 1)
	if rem2.Cmp(den) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	neg := (a.neg != b.neg) && q.Sign() != 0
	return pgDecimal{
		kind:  pgNumFinite,
		neg:   neg,
		coeff: q,
		scale: rscale,
	}, nil
}

// PGNumericAdd adds two PostgreSQL numeric strings with exact decimal semantics.
func PGNumericAdd(a, b string) (string, error) {
	da, err := parsePGDecimal(a)
	if err != nil {
		return "", err
	}
	db, err := parsePGDecimal(b)
	if err != nil {
		return "", err
	}
	return formatPGDecimal(pgDecimalAdd(da, db)), nil
}

// PGNumericSub subtracts two PostgreSQL numeric strings with exact decimal semantics.
func PGNumericSub(a, b string) (string, error) {
	da, err := parsePGDecimal(a)
	if err != nil {
		return "", err
	}
	db, err := parsePGDecimal(b)
	if err != nil {
		return "", err
	}
	return formatPGDecimal(pgDecimalSub(da, db)), nil
}

// PGNumericMul multiplies two PostgreSQL numeric strings with exact decimal semantics.
func PGNumericMul(a, b string) (string, error) {
	da, err := parsePGDecimal(a)
	if err != nil {
		return "", err
	}
	db, err := parsePGDecimal(b)
	if err != nil {
		return "", err
	}
	return formatPGDecimal(pgDecimalMul(da, db)), nil
}

// PGNumericDiv divides two PostgreSQL numeric strings with exact decimal semantics.
func PGNumericDiv(a, b string) (string, error) {
	da, err := parsePGDecimal(a)
	if err != nil {
		return "", err
	}
	db, err := parsePGDecimal(b)
	if err != nil {
		return "", err
	}
	rscale := 16
	if da.scale > rscale {
		rscale = da.scale
	}
	if db.scale+4 > rscale {
		rscale = db.scale + 4
	}
	res, err := pgDecimalDiv(da, db, rscale)
	if err != nil {
		return "", err
	}
	return formatPGDecimal(res), nil
}

// pgNumericAgg accumulates exact numeric values for SUM() and AVG().
type pgNumericAgg struct {
	sum   pgDecimal
	count int64
}

func newPGNumericAgg() *pgNumericAgg {
	return &pgNumericAgg{
		sum: pgDecimal{kind: pgNumFinite, coeff: big.NewInt(0), scale: 0},
	}
}

func (agg *pgNumericAgg) Step(val string) error {
	d, err := parsePGDecimal(val)
	if err != nil {
		return err
	}
	if agg.count == 0 {
		agg.sum = d
	} else {
		agg.sum = pgDecimalAdd(agg.sum, d)
	}
	agg.count++
	return nil
}

func (agg *pgNumericAgg) SumResult() (string, bool) {
	if agg.count == 0 {
		return "", false
	}
	return formatPGDecimal(agg.sum), true
}

func (agg *pgNumericAgg) AvgResult() (string, bool, error) {
	if agg.count == 0 {
		return "", false, nil
	}
	if agg.sum.kind != pgNumFinite {
		return formatPGDecimal(agg.sum), true, nil
	}
	rscale := 16
	if agg.sum.scale+4 > rscale {
		rscale = agg.sum.scale + 4
	}
	cntDec := pgDecimal{kind: pgNumFinite, coeff: big.NewInt(agg.count), scale: 0}
	res, err := pgDecimalDiv(agg.sum, cntDec, rscale)
	if err != nil {
		return "", false, err
	}
	return formatPGDecimal(res), true, nil
}
