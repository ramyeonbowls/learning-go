package worker

import (
	"context"
	"strings"
	"sync"
	"time"

	"txt-to-sqlserver/internal/model"
)

func ParseSdealWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	lines <-chan string,
	ch130 chan<- model.Sdeal130Hdr,
	ch131 chan<- model.Sdeal131Det,
	ch132 chan<- model.Sdeal132Mix,
	ch120 chan<- model.Sdeal120Hdr,
	ch121 chan<- model.Sdeal121Itm,
	ch122 chan<- model.Sdeal122Det,
	ch123 chan<- model.Sdeal123Mix,
	ch124 chan<- model.Sdeal124Reg,
) {
	defer wg.Done()

	lineNumber := 0
	fileName := "data/SDEAL_20251218_210045.txt" // bisa dibuat dinamis

	for {
		select {
		case <-ctx.Done():
			return

		case line, ok := <-lines:
			if !ok {
				return
			}

			lineNumber++

			// ===== split cepat =====
			fields := strings.Split(line, "|")
			if len(fields) < 2 {
				continue
			}

			blockID := strings.TrimSpace(fields[0])
			blockCode := strings.TrimSpace(fields[1])
			now := time.Now()

			switch blockID {

			// =====================================================
			// 130 | ZFRHDR
			// =====================================================
			case "130":
				/* 130|ZFRHDR|ZNA0|F1|0400|10|13|B491||||||1100149565|ATP00003|20260531|20251001|0000029208|*/

				rec := model.Sdeal130Hdr{
					BlockID:             blockID,
					BlockCode:           blockCode,
					ConditionType:       safe(fields, 2),
					KeyCombination:      safe(fields, 3),
					KeyComb:             safe(fields, 4),
					SalesOrganization:   safe(fields, 5),
					DistributionChannel: safe(fields, 6),
					SalesOffice:         safe(fields, 7),
					Division:            safe(fields, 8),
					PaymentTerm:         safe(fields, 9),
					Customer:            safe(fields, 12),
					Material:            safe(fields, 13),
					Attribut2:           "",
					ValidUntil:          safe(fields, 14),
					ValidFrom:           safe(fields, 15),
					ConditionRecordNo:   safe(fields, 16),
					FileName:            fileName,
					LineNumber:          lineNumber,
					CDate:               now,
				}

				ch130 <- rec

			// =====================================================
			// 131 | ZFRDET
			// =====================================================
			case "131":
				/* 131|ZFRDET|0000041809| 1| 1|CAR| 1|CAR|ATK00005 */

				rec := model.Sdeal131Det{
					BlockID:           blockID,
					BlockCode:         blockCode,
					ConditionRecordNo: safe(fields, 2),
					Scale:             safe(fields, 3),
					Unit:              safe(fields, 4),
					Amount:            safe(fields, 5),
					Currency:          safe(fields, 6),
					FileName:          fileName,
					LineNumber:        lineNumber,
					CDate:             now,
				}

				ch131 <- rec

			// =====================================================
			// 132 | ZFRMIX
			// =====================================================
			case "132":
				/* 132|ZFRMIX|PKL_TPH20| 1| 2|BOS|AP000004| 1|BOS| 0,00| */

				rec := model.Sdeal132Mix{
					BlockID:    blockID,
					BlockCode:  blockCode,
					MixCode:    safe(fields, 2),
					SeqNo:      safe(fields, 3),
					LevelNo:    safe(fields, 4),
					Plant:      safe(fields, 5),
					Material:   safe(fields, 6),
					Scale:      safe(fields, 7),
					FileName:   fileName,
					LineNumber: lineNumber,
					CDate:      now,
				}

				ch132 <- rec

			// =====================================================
			// 120 | ZFRHDR
			// =====================================================
			case "120":
				/* 120|ZDHDR|ZDH7|H2|0400|10|B351|15||1100025836|||20991231|20190801|0000013929| */

				rec := model.Sdeal120Hdr{
					BlockID:             blockID,
					BlockCode:           blockCode,
					ConditionType:       safe(fields, 2),
					SalesOrganization:   safe(fields, 5),
					DistributionChannel: safe(fields, 6),
					Customer:            safe(fields, 7),
					ValidUntil:          safe(fields, 12),
					ValidFrom:           safe(fields, 13),
					ConditionRecordNo:   safe(fields, 14),
					FileName:            fileName,
					LineNumber:          lineNumber,
					CDate:               now,
				}

				ch120 <- rec

			// =====================================================
			// 121 | ZDITM
			// =====================================================
			case "121":
				/* 121|ZDITM|ZDF2|I1|0400|10|B376|15|1100159366|AJP00001|||||||AJP00001||20251231|20251001|0003525830|C */

				rec := model.Sdeal121Itm{
					BlockID:             blockID,
					BlockCode:           blockCode,
					Material:            safe(fields, 7),
					SalesOrganization:   safe(fields, 5),
					DistributionChannel: safe(fields, 6),
					Customer:            safe(fields, 8),
					ValidUntil:          safe(fields, 18),
					ValidFrom:           safe(fields, 19),
					ConditionRecordNo:   safe(fields, 20),
					Flag:                safe(fields, 21),
					FileName:            fileName,
					LineNumber:          lineNumber,
					CDate:               now,
				}

				ch121 <- rec

			// =====================================================
			// 122 | ZDDET
			// =====================================================
			case "122":
				/* 122|ZDDET|0003534841| 3-|%||KRT|C */

				rec := model.Sdeal122Det{
					BlockID:    blockID,
					BlockCode:  blockCode,
					Value:      safe(fields, 2),
					PercentFlg: safe(fields, 3),
					Plant:      safe(fields, 5),
					Flag:       safe(fields, 6),
					FileName:   fileName,
					LineNumber: lineNumber,
					CDate:      now,
				}

				ch122 <- rec

			// =====================================================
			// 123 | ZPMIX
			// =====================================================
			case "123":
				/* 123|ZPMIX|ZDR2|A5|0400|10|B301|32||||||||A0000034|20251231|20251001|JBR0950003| 12||||||||||||||X|1100121884|H */

				rec := model.Sdeal123Mix{
					BlockID:             blockID,
					BlockCode:           blockCode,
					ConditionType:       safe(fields, 2),
					SalesOrganization:   safe(fields, 5),
					DistributionChannel: safe(fields, 6),
					Customer:            safe(fields, 7),
					Material:            safe(fields, 8),
					ValidUntil:          safe(fields, 15),
					ValidFrom:           safe(fields, 16),
					ConditionRecordNo:   safe(fields, 17),
					Qty:                 safe(fields, 18),
					Plant:               safe(fields, 19),
					Flag:                safe(fields, 20),
					FileName:            fileName,
					LineNumber:          lineNumber,
					CDate:               now,
				}

				ch123 <- rec

			// =====================================================
			// 124 | ZDREG
			// =====================================================
			case "124":
				/* 124|ZSCREG|0003528652|01|0001| 1| 5-|% */

				rec := model.Sdeal124Reg{
					BlockID:           blockID,
					BlockCode:         blockCode,
					ConditionRecordNo: safe(fields, 2),
					ScaleNo:           safe(fields, 3),
					Unit:              safe(fields, 4),
					Rate:              safe(fields, 5),
					Currency:          safe(fields, 6),
					FileName:          fileName,
					LineNumber:        lineNumber,
					CDate:             now,
				}

				ch124 <- rec

			default:
				// skip block yang belum dipakai
			}
		}
	}
}

/*
=====================================================
 Helper function
=====================================================
*/

func safe(arr []string, idx int) string {
	if idx >= len(arr) {
		return ""
	}
	return strings.TrimSpace(arr[idx])
}
