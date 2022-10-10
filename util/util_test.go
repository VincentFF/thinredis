package util

import "testing"

func TestPattenMatch(t *testing.T) {
	p1, s1 := "", "a"
	p2, s2 := "a", ""
	p3, s3, s3_1, s3_2 := "h?llo", "hello", "hallo", "haxllo"
	p4, s4, s4_1, s4_2, s4_3 := "h*llo", "hello", "haabbllo", "hllo", "ahello"
	p5, s5, s5_1, s5_2 := "h[ae]llo", "hello", "hallo", "hillo"
	p6, s6, s6_1 := "h[^e]llo", "hallo", "hello"
	p7, s7, s7_1, s7_2 := "h[a-c]llo", "hallo", "hbllo", "hdllo"
	p8, s8 := "h[e-]llo", "hello"
	p9, s9 := "h[ello", "hello"

	if PattenMatch(p1, s1) {
		t.Error("PattenMatch(\"\", \"a\") should return false")
	}
	if PattenMatch(p2, s2) {
		t.Error("PattenMatch(\"a\", \"\") should return false")
	}
	if !PattenMatch(p3, s3) {
		t.Error("PattenMatch(\"h?llo\", \"hello\") should return true")
	}
	if !PattenMatch(p3, s3_1) {
		t.Error("PattenMatch(\"h?llo\", \"hallo\") should return true")
	}
	if PattenMatch(p3, s3_2) {
		t.Error("PattenMatch(\"h?llo\", \"haxllo\") should return false")
	}
	if !PattenMatch(p4, s4) {
		t.Error("PattenMatch(\"h*llo\", \"hello\") should return true")
	}
	if !PattenMatch(p4, s4_1) {
		t.Error("PattenMatch(\"h*llo\", \"haabbllo\") should return true")
	}
	if !PattenMatch(p4, s4_2) {
		t.Error("PattenMatch(\"h*llo\", \"hllo\") should return true")
	}
	if PattenMatch(p4, s4_3) {
		t.Error("PattenMatch(\"h*llo\", \"ahello\") should return false")
	}
	if !PattenMatch(p5, s5) || !PattenMatch(p5, s5_1) {
		t.Error("PattenMatch(\"h[ae]llo\", \"hello\") and PattenMatch(\"h[ae]llo\", \"hallo\") should return true")
	}
	if PattenMatch(p5, s5_2) {
		t.Error("PattenMatch(\"h[ae]llo\", \"hillo\") should return false")
	}
	if !PattenMatch(p6, s6) {
		t.Error("PattenMatch(\"h[^e]llo\", \"hallo\") should return true")
	}
	if PattenMatch(p6, s6_1) {
		t.Error("PattenMatch(\"h[^e]llo\", \"hello\") should return false")
	}
	if !PattenMatch(p7, s7) || !PattenMatch(p7, s7_1) {
		t.Error("PattenMatch(\"h[a-c]llo\", \"hallo\") and PattenMatch(\"h[a-c]llo\", \"hbllo\") should return true")
	}
	if PattenMatch(p7, s7_2) {
		t.Error("PattenMatch(\"h[a-c]llo\", \"hdllo\") should return false")
	}
	if PattenMatch(p8, s8) {
		t.Error("PattenMatch(\"h[e-]llo\", \"hello\") should return false")
	}
	if PattenMatch(p9, s9) {
		t.Error("PattenMatch(\"h[ello\", \"hello\") should return false")
	}
}
