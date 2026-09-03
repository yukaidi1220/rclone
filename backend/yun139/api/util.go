// Package api contains the HTTP type definitions and shared helpers for the
// yun139 (中国移动云盘) backend.
package api

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/url"
	"sort"
	"strings"
)

// errInvalidToken is returned when an authorization token cannot be decoded.
var errInvalidToken = errors.New("yun139: invalid authorization token")

// encodeURIComponent escapes s the way the 139 web client does: like
// url.QueryEscape but leaving ! ' ( ) * unescaped and using %20 for spaces.
func encodeURIComponent(s string) string {
	r := url.QueryEscape(s)
	r = strings.Replace(r, "+", "%20", -1)
	r = strings.Replace(r, "%21", "!", -1)
	r = strings.Replace(r, "%27", "'", -1)
	r = strings.Replace(r, "%28", "(", -1)
	r = strings.Replace(r, "%29", ")", -1)
	r = strings.Replace(r, "%2A", "*", -1)
	return r
}

// md5Hex returns the lower-case hex MD5 of s.
func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// calSign computes the mcloud-sign hash for a request body.
//
// Algorithm (verified against alist / OpenList / cloud139):
//  1. URI-encode the body, split into characters and sort them
//  2. base64 the sorted string
//  3. sign = MD5(MD5(base64) + MD5(ts + ":" + randStr))
//  4. uppercase the result
func calSign(body, ts, randStr string) string {
	body = encodeURIComponent(body)
	chars := strings.Split(body, "")
	sort.Strings(chars)
	body = strings.Join(chars, "")
	body = base64.StdEncoding.EncodeToString([]byte(body))
	res := md5Hex(body) + md5Hex(ts+":"+randStr)
	return strings.ToUpper(md5Hex(res))
}

// Sign is the exported wrapper of calSign.
func Sign(body, ts, randStr string) string {
	return calSign(body, ts, randStr)
}

// AuthFromToken builds the "Basic " authorization header value from an
// existing base64 authorization token. The raw token is
// base64("pc:<account>:<token>|<...>"); the header is "Basic " + raw.
func AuthFromToken(authorization string) string {
	if authorization == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(authorization), "basic ") {
		return authorization
	}
	return "Basic " + authorization
}

// DecodeToken splits a base64 authorization token into its components.
// Returns account and the raw token string (splits[2], the part after the
// second colon).
func DecodeToken(authorization string) (account, token string, err error) {
	decoded, err := base64.StdEncoding.DecodeString(authorization)
	if err != nil {
		return "", "", err
	}
	parts := strings.SplitN(string(decoded), ":", 3)
	if len(parts) < 3 {
		return "", "", errInvalidToken
	}
	return parts[1], parts[2], nil
}