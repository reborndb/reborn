# github.com/xiam/resp

[RESP][1] encoder/decoder.

## Installation

```
go get -u github.com/xiam/resp
```

## Usage

### Encoding

```
func Marshal(v interface{}) ([]byte, error)
    Marshal returns the RESP encoding of v. At this moment, it only works
    with string, int, []byte, nil and []interface{} types.
```

Example:

```
buf, err = resp.Marshal("Foo")
// -> $3\r\nFoo\r\n
```

### Decoding

```
func Unmarshal(data []byte, v interface{}) error
    Unmarshal parses the RESP-encoded data and stores the result in the
    value pointed to by v. At this moment, it only works with string, int,
    []byte and []interface{} types.
```

Example:

```
var dest string
err = resp.Unmarshal([]byte("$3\r\nFoo\r\n"), &dest)
// -> "Foo"
```

## License

> Copyright (c) 2014 José Carlos Nieto, https://menteslibres.net/xiam
>
> Permission is hereby granted, free of charge, to any person obtaining
> a copy of this software and associated documentation files (the
> "Software"), to deal in the Software without restriction, including
> without limitation the rights to use, copy, modify, merge, publish,
> distribute, sublicense, and/or sell copies of the Software, and to
> permit persons to whom the Software is furnished to do so, subject to
> the following conditions:
>
> The above copyright notice and this permission notice shall be
> included in all copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
> EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
> MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
> NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
> LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
> OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
> WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

[1]: http://redis.io/topics/protocol
