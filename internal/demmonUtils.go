package internal

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	"github.com/mitchellh/hashstructure/v2"
	"github.com/mitchellh/mapstructure"
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/sirupsen/logrus"
)

func (d *Demmon) readPump(c *client) {
	defer func() {
		close(c.done)
		err := c.conn.Close()
		if err != nil {
			d.logger.Errorf("error closing connection: %s", err.Error())
		}
	}()

	for {
		select {
		case <-c.done:
		default:
			req := &body_types.Request{}
			err := c.conn.ReadJSON(req)
			if err != nil {
				d.logger.Errorf("error: %v reading from connection", err)
				return
			}
			d.handleRequest(req, c)
		}
	}
}

func (d *Demmon) writePump(c *client) {
	defer func() {
		err := c.conn.Close()
		if err != nil {
			d.logger.Errorf("error closing the connection: %s", err.Error())
		}
	}()
	for {
		select {
		case msg, ok := <-c.out:
			if !ok {
				return
			}
			err := c.conn.WriteJSON(msg)
			if err != nil {
				d.logger.Errorf("error: %v writing to connection", err)
				return
			}
		case <-c.done:
		}
	}
}

func (d *Demmon) extractBody(r *body_types.Request, reqBody interface{}, resp *body_types.Response) bool {
	err := decode(r.Message, reqBody)
	if err != nil {
		d.logger.Error(err)
		resp.Code = 400
		resp.ID = r.ID
		resp.Error = true
		resp.Type = r.Type
		resp.Message = body_types.ErrBadBodyType
		return false
	}

	return true
}

type formatter struct {
	owner string
	lf    logrus.Formatter
}

func (f *formatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Message = fmt.Sprintf("[%s] %s", f.owner, e.Message)
	return f.lf.Format(e)
}

func setupLogger(logger *logrus.Logger, logFolder, logFile string, silent bool) {
	logger.SetFormatter(
		&formatter{
			owner: "Demmon_Frontend",
			lf: &logrus.TextFormatter{
				DisableColors:   true,
				ForceColors:     false,
				FullTimestamp:   true,
				TimestampFormat: time.StampMilli,
			},
		},
	)

	if logFolder == "" {
		logger.Panicf("Invalid logFolder '%s'", logFolder)
	}

	if logFile == "" {
		logger.Panicf("Invalid logFile '%s'", logFile)
	}

	filePath := fmt.Sprintf("%s/%s", logFolder, logFile)
	err := os.MkdirAll(logFolder, 0777)
	if err != nil {
		logger.Panic(err)
	}
	file, err := os.Create(filePath)
	if os.IsExist(err) {
		var err = os.Remove(filePath)
		if err != nil {
			logger.Panic(err)
		}
		file, err = os.Create(filePath)
		if err != nil {
			logger.Panic(err)
		}
	}
	var out io.Writer = file
	if !silent {
		out = io.MultiWriter(os.Stdout, file)

		fmt.Println("Setting metrics_frontend non-silently")
	}
	logger.SetOutput(out)
}

func decode(input, result interface{}) error {
	decoder, err := mapstructure.NewDecoder(
		&mapstructure.DecoderConfig{
			Metadata: nil,
			TagName:  "json",
			DecodeHook: mapstructure.ComposeDecodeHookFunc(
				toTimeHookFunc(),
				mapstructure.StringToIPHookFunc(),
			),
			Result: result,
		},
	)
	if err != nil {
		return err
	}
	err = decoder.Decode(input)
	if err != nil {
		return err
	}
	return err
}

func toTimeHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if t != reflect.TypeOf(time.Time{}) {
			return data, nil
		}

		switch f.Kind() {
		case reflect.String:
			return time.Parse(time.RFC3339, data.(string))
		case reflect.Float64:
			return time.Unix(0, int64(data.(float64))*int64(time.Millisecond)), nil
		case reflect.Int64:
			return time.Unix(0, data.(int64)*int64(time.Millisecond)), nil
		default:
			return data, nil
		}
		// Convert it by parsing
	}
}

func Hash(v interface{}) uint64 {
	hash, err := hashstructure.Hash(v, hashstructure.FormatV2, nil)
	if err != nil {
		panic(err)
	}
	return hash
}
