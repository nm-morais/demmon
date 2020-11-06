package plugin_manager

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"plugin"
	"strings"
	"sync"

	"github.com/nm-morais/demmon-common/body_types"
)

var (
	ErrSymbolNotFound = errors.New("plugin symbol not found")
	ErrPluginNotFound = errors.New("plugin not found")
	ErrAlreadyExists  = errors.New("plugin already exists")
)

var pm *PluginManager

const compilationFolder = "compilation_folder"

type PluginManagerConfig struct {
	WorkingDir string
}

type PluginManager struct {
	loadedPlugins *sync.Map
	conf          PluginManagerConfig
	compileLock   *sync.Mutex
}

func New(conf PluginManagerConfig) *PluginManager {
	pm = &PluginManager{
		loadedPlugins: &sync.Map{},
		conf:          conf,
		compileLock:   &sync.Mutex{},
	}
	err := os.MkdirAll(conf.WorkingDir, 0777)
	if err != nil {
		panic("Could not create plugin dir")
	}
	err = os.MkdirAll(fmt.Sprintf("%s/%s", conf.WorkingDir, compilationFolder), 0777)
	if err != nil {
		panic("Could not create plugin dir")
	}
	return pm
}

func (pm *PluginManager) CompileAndStorePlugin(pluginName string) (*plugin.Plugin, error) {
	_ = os.RemoveAll(fmt.Sprintf("%s/%s/*", pm.conf.WorkingDir, compilationFolder))
	_, ok := pm.GetPlugin(pluginName)
	if ok {
		return nil, ErrAlreadyExists
	}
	pm.compileLock.Lock()
	defer pm.compileLock.Unlock()
	srcFileName := fmt.Sprintf("%s/%s.go", pm.conf.WorkingDir, pluginName)
	tmpFileName := fmt.Sprintf("%s/%s/%s.go", pm.conf.WorkingDir, compilationFolder, pluginName)
	fmt.Println("copying file...")
	sourceFileStat, err := os.Stat(srcFileName)
	if err != nil {
		return nil, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", srcFileName)
	}

	source, err := os.Open(srcFileName)
	if err != nil {
		return nil, err
	}

	defer source.Close()

	destination, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	sc := bufio.NewScanner(source)
	for sc.Scan() {
		txt := sc.Text() // GET the line string
		if strings.Contains(txt, "package ") {
			fmt.Printf("Writing line %s\n", "package main")
			destination.WriteString("package main\n")
			continue
		}
		fmt.Printf("Writing line %s\n", txt)
		destination.WriteString(fmt.Sprintf("%s\n", txt))
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("scan file error: %v", err)
		return nil, err
	}
	destination.Close()
	source.Close()
	outPutFile := fmt.Sprintf("%s/%s.so", pm.conf.WorkingDir, pluginName)
	command := exec.Command("go", "build", "-o", outPutFile, "-buildmode=plugin", tmpFileName)
	fmt.Println(command.String())
	command.Env = os.Environ()
	command.Env = append(command.Env, "GO111MODULE=off")
	out, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("err: %s, output: %s", err.Error(), out)
	}
	// libHandle, err := dl.Open(fmt.Sprintf("%s/%s.so", pm.conf.WorkingDir, pluginName), dl.RTLD_LAZY)
	// if err != nil {
	// 	return nil, fmt.Errorf("err getting handle: %s", err.Error())
	// }
	plugin, err := plugin.Open(fmt.Sprintf("%s/%s.so", pm.conf.WorkingDir, pluginName))
	if err != nil {
		return nil, err
	}
	pm.loadedPlugins.Store(pluginName, plugin)
	// err = copyFile(outPutFile, fmt.Sprintf("%s/%s.so", pm.conf.WorkingDir, pluginName))
	// if err != nil {
	// 	return err
	// }
	return plugin, nil
}

func (pm *PluginManager) GetPlugin(pluginName string) (*plugin.Plugin, bool) {
	if p, ok := pm.loadedPlugins.Load(pluginName); ok {
		return p.(*plugin.Plugin), ok
	}
	return nil, false
}

func (pm *PluginManager) GetInstalledPlugins() []string {
	toSend := []string{}
	pm.loadedPlugins.Range(func(key, value interface{}) bool {
		toSend = append(toSend, key.(string))
		return true
	})
	return toSend
}

func (pm *PluginManager) GetPluginSymbol(pluginName, symbolName string) (plugin.Symbol, error) {
	p, ok := pm.GetPlugin(pluginName)
	if !ok {
		return nil, ErrPluginNotFound
	}
	fmt.Printf("Looking up symbol %s in plugin %+v\n", symbolName, p)
	s, err := p.Lookup(symbolName)
	if err != nil {
		return nil, ErrSymbolNotFound
	}
	return s, nil
}

func (pm *PluginManager) AddPluginChunk(p *body_types.PluginFileBlock) error {
	if p.FinalBlock {
		if _, err := pm.CompileAndStorePlugin(p.Name); err != nil {
			return err
		}
	}
	codeFilePath := fmt.Sprintf("%s/%s.go", pm.conf.WorkingDir, p.Name)
	f, err := pm.getOrCreatePluginCodeFile(codeFilePath, p.FirstBlock)
	if err != nil {
		return err
	}
	defer f.Close()

	// Copy the uploaded file to the created file on the filesystem
	pluginBytes, err := base64.StdEncoding.DecodeString(p.Content)
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, bytes.NewReader(pluginBytes)); err != nil {
		return err
	}
	return nil
}

func (pm *PluginManager) getOrCreatePluginCodeFile(codeFilePath string, create bool) (*os.File, error) {
	if create {
		_ = os.Remove(codeFilePath)
		dst, err := os.Create(codeFilePath)
		if err != nil {
			return dst, err
		}
	}
	file, err := os.OpenFile(codeFilePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func copyFile(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()
	_, err = io.Copy(destination, source)
	return err
}
