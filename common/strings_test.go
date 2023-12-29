package common

import "testing"

func SpecialLettersEquals(bs []byte) bool {
	bs1 := SpecialLettersUsingMySQL(bs)
	bs2 := SpecialLettersUsingMySQLOld(bs)
	return bs2 == bs1
}

func TestSpecialLettersEquals(t *testing.T) {

	bs1 := make([]byte, 256)
	for i := 0; i < 256; i++ {
		bs1[i] = byte(i)
	}

	type args struct {
		bs []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{bs: bs1},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SpecialLettersEquals(tt.args.bs); got != tt.want {
				t.Errorf("SpecialLettersEquals() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkSpecialLettersUsingMySQLOld(b *testing.B) {
	bs1 := make([]byte, 256)
	for i := 0; i < 256; i++ {
		bs1[i] = byte(i)
	}
	// Call b.ReportAllocs() to enable memory allocation tracking
	b.ReportAllocs()

	// Run the function b.N times
	for i := 0; i < b.N; i++ {
		SpecialLettersUsingMySQLOld(bs1)
	}
}

func BenchmarkSpecialLettersUsingMySQLNew(b *testing.B) {
	bs1 := make([]byte, 256)
	for i := 0; i < 256; i++ {
		bs1[i] = byte(i)
	}
	// Call b.ReportAllocs() to enable memory allocation tracking
	b.ReportAllocs()

	// Run the function b.N times
	for i := 0; i < b.N; i++ {
		SpecialLettersUsingMySQL(bs1)
	}
}
