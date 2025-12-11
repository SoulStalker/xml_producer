package processor

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SoulStalker/xml_producer/internal/producer"
)

type FileProcessor struct {
	nfsPath       string
	backupPath    string
	retentionTime time.Duration
	producer      producer.MessageProducer
}

// type MessageSender interface {
// 	SendMessage(ctx context.Context, fileName string, data []byte) error
// }

func NewFileProcessor(nfsPath, backupPath string, retentionTime time.Duration, producer producer.MessageProducer) (*FileProcessor, error) {
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %v", err)
	}

	return &FileProcessor{
		nfsPath:       nfsPath,
		backupPath:    backupPath,
		retentionTime: retentionTime,
		producer:      producer,
	}, nil
}

func (p *FileProcessor) Process(ctx context.Context) error {
	files, err := os.ReadDir(p.nfsPath)
	if err != nil {
		return fmt.Errorf("failed to read NFS directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if !strings.HasSuffix(strings.ToLower(file.Name()), ".xml") {
			continue
		}

		filePath := filepath.Join(p.nfsPath, file.Name())
		if err := p.processFile(ctx, filePath, file.Name()); err != nil {
			log.Printf("Error processing file %s: %v", file.Name(), err)
			continue
		}
	}

	return nil
}

func (p *FileProcessor) processFile(ctx context.Context, filePath, fileName string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	if err := p.producer.SendMessage(ctx, fileName, data); err != nil {
		return fmt.Errorf("send file via producer: %w", err)
	}

	backupFilePath := filepath.Join(p.backupPath, fileName)
	if err := os.Rename(filePath, backupFilePath); err != nil {
		return fmt.Errorf("move file to backup: %w", err)
	}

	log.Printf("Successfully processed and backed up file: %s", fileName)
	return nil
}

func (p *FileProcessor) CleanupOldBackups() error {
	files, err := os.ReadDir(p.backupPath)
	if err != nil {
		return fmt.Errorf("failed to read backup directory: %v", err)
	}

	now := time.Now()
	deletedCnt := 0

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		entry, err := file.Info()
		if err != nil {
			log.Printf("failed to stat file %s: %v", file.Name(), err)
		}

		fileAge := now.Sub(entry.ModTime())
		if fileAge > p.retentionTime {
			filePath := filepath.Join(p.backupPath, file.Name())
			if err := os.Remove(filePath); err != nil {
				log.Printf("Failed to delete old backup file %s: %v", file.Name(), fileAge)
				continue
			}
			deletedCnt++
			log.Printf("Deleted old backup file: %s (age: %v)", file.Name(), fileAge)
		}
	}

	if deletedCnt > 0 {
		log.Printf("Cleanup completed: deleted %d old backup files", deletedCnt)
	}

	return nil
}
