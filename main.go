package main

import (
    "log"
	"net"
	"os"
	"bufio"
	"strings"
	"errors"
	"io"
	"strconv"
	"path/filepath"
)

var (
	passwd string
	db *DB
	dbPath string
)

func main() {
	argc:=len(os.Args)
	if argc <=2 {
		log.Println("bolt-server [Listen](tcp://127.0.0.1:6379) [passwd]")
		return
	}
	proto := strings.SplitN(os.Args[1], "://", 2)
	if len(proto) != 2 {
		log.Println("bolt-server [Listen](tcp://127.0.0.1:6379) [passwd]")
		return
	}
    if proto[0]=="unix"{
		os.Remove(proto[1])
	}
	passwd=os.Args[2]
	var err error
	dbPath=findableDir()
	db, err = Open(filepath.Join(dbPath,"sqlite.db"), 0600, nil)
    defer db.Close()
	if err!=nil {
		log.Print(err)
		return
	}
	
	if len(passwd) <8 {
		log.Print("ERR: password len < 8")
		return
	}
	
	log.Printf("bolt-server: Listening %s \n", proto[1])

    l, err := net.Listen(proto[0], proto[1])
    if err != nil {
        log.Print(err)
		return
    }
    defer l.Close()

    for {
        c, err := l.Accept()
        if err != nil {
            log.Println("accept err:", err)
            continue
        }
        go handle(c)
    }
}

func handle(conn net.Conn) {
    defer conn.Close()

    reader := bufio.NewReader(conn)
    authenticated := false

    for {
        args, err := readRESP(reader)
        if err != nil {
			log.Println("resolv RESP err:",err)
            return
        }

        if len(args) == 0 {
            writeError(conn, "ERR Protocol error: invalid multibulk length")
            continue
        }

        cmd := strings.ToUpper(args[0])

        switch cmd {

        case "AUTH":
            if len(args) < 2 {
                writeError(conn, "WRONGPASS invalid username-password pair or user is disabled.")
                continue
            }

            if args[1] == passwd {
                authenticated = true
                writeSimpleString(conn, "OK")
            } else {
                writeError(conn, "WRONGPASS invalid username-password pair or user is disabled.")
            }

        case "SET":
            if !authenticated {
                writeError(conn, "NOAUTH Authentication required.")
                continue
            }

            if len(args) != 3 {
                writeError(conn, "ERR wrong number of arguments for 'set'")
                continue
            }
			
			var errs error
			db.Update(func(tx *Tx) error {
				b, _ := tx.CreateBucketIfNotExists([]byte("sql"))
				errs=b.Put([]byte(args[1]), []byte(args[2]))
				return errs
			})
			
			if errs == nil {
				writeSimpleString(conn, "OK")
			} else {
				writeError(conn, errs.Error())
			}

        case "GET":
            if !authenticated {
                writeError(conn, "NOAUTH Authentication required.")
                continue
            }

            if len(args) != 2 {
                writeError(conn, "ERR wrong number of arguments for 'get'")
                continue
            }
			
			val:=""
			
			db.View(func(tx *Tx) error {
				b := tx.Bucket([]byte("sql"))
				if b == nil {
					return nil
				}
				v := b.Get([]byte(args[1]))
				if v == nil {
					return nil
				}
				val=string(v)
				return nil
			})


            if val=="" {
                writeNull(conn)
            } else {
                writeBulkString(conn, val)
            }

        case "PING":
			if !authenticated {
                writeError(conn, "NOAUTH Authentication required.")
                continue
            }
			writeBulkString(conn, "PONG")
		
		case "INFO":
			if !authenticated {
                writeError(conn, "NOAUTH Authentication required.")
                continue
            }
			writeBulkString(conn, "bolt-server v1.0 (https://github.com/stalltrix/bolt-server), dbPath on: "+ dbPath)
		default:
            writeError(conn, "ERR unknown command")
        }
    }
}

func readRESP(r *bufio.Reader) ([]string, error) {
    line, err := r.ReadSlice('\n')
    if err != nil {
        return nil, err
    }

    if len(line) < 3 || line[0] != '*' {
        return nil, errors.New("invalid resp")
    }

    n := 0
    for i := 1; i < len(line); i++ {
        c := line[i]
        if c == '\r' {
            break
        }
        n = n*10 + int(c-'0')
    }

    args := make([]string, n)
    for i := 0; i < n; i++ {
        line, err = r.ReadSlice('\n')
        if err != nil {
            return nil, err
        }

        if line[0] != '$' {
            return nil, errors.New("invalid bulk string")
        }
        size := 0
        for j := 1; j < len(line); j++ {
            c := line[j]
            if c == '\r' {
                break
            }
            size = size*10 + int(c-'0')
        }
        buf := make([]byte, size+2)
        _, err = io.ReadFull(r, buf)
        if err != nil {
            return nil, err
        }

        args[i] = string(buf[:size])
    }

    return args, nil
}

func writeSimpleString(conn net.Conn, s string) {
    io.WriteString(conn, "+"+s+"\r\n")
}

func writeError(conn net.Conn, s string) {
   io.WriteString(conn, "-"+s+"\r\n")
}

func writeBulkString(conn net.Conn, s string) {
    io.WriteString(conn, "$"+strconv.Itoa(len(s))+"\r\n"+s+"\r\n")
}

func writeNull(conn net.Conn) {
    io.WriteString(conn, "$-1\r\n")
}

func findableDir() string {
	home:=""
	exePath, err := os.Executable()
	if err == nil {
		home = filepath.Dir(exePath)
	}
	可写:=DirTest(home)
	if home == "" || !可写  {
		home,err := os.UserHomeDir()
		if err == nil {
			可写=DirTest(home)
		}
		if home == "" || !可写 {
		home = os.Getenv("HOME")
		可写=DirTest(home)
        if home == "" || !可写 {
			home = os.Getenv("USERPROFILE")
			可写=DirTest(home)
			if !可写 {
				home = os.TempDir()
			}
		}
    }}
	return home
}

func DirTest(dir string) bool {
	info, err := os.Stat(dir)
	if err != nil {
		return false 
	}
	if !info.IsDir() {
		return false
	}
	temp, err := os.CreateTemp(dir, ".permcheck-*")
	if err != nil {
		return false
	}
	name := temp.Name()
	temp.Close()
	os.Remove(name)
	return true
}