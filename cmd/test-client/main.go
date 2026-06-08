// Package main - SD-03 分布式缓存系统 CLI 交互式 TCP 测试客户端
//
// 运行方式:
//
//	go run ./cmd/test-client/
//
// 三大功能模块:
//  1. 简易测试 - 三级菜单自动执行（模块 → 场景 → 用例）
//  2. 自由测试 - 动态指令全功能覆盖
//  3. 客户端设置 - 自动保存/配置/超时
package main

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

func main() {
	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("  SD-03 分布式缓存系统 - TCP 测试客户端 v1.0")
	fmt.Println("  Protocol | Cache | Shard | Node | Server | Replication")
	fmt.Println("============================================================")
	fmt.Println("  提示: 输入数字选择菜单，输入 0 或 q 退出")
	fmt.Println()

	reader := bufio.NewReader(os.Stdin)
	cli := NewCLIClient(reader)

	defer func() {
		cli.Cleanup()
		fmt.Println("\n  测试客户端已关闭，再见!")
	}()

	// 进入新层级时清屏并显示菜单
	if cli.settings.DisplayMode >= 2 {
		clearScreen()
	}
	cli.ShowMainMenu()

	for {
		choice := readInput(reader, "请选择: ")

		switch choice {
		case "1":
			cli.RunAutoTests(reader)
			// 返回后重新显示主菜单
			if cli.settings.DisplayMode >= 2 {
				clearScreen()
			}
			cli.ShowMainMenu()
		case "2":
			cli.RunFreeMode()
			if cli.settings.DisplayMode >= 2 {
				clearScreen()
			}
			cli.ShowMainMenu()
		case "3":
			cli.RunSettings()
			if cli.settings.DisplayMode >= 2 {
				clearScreen()
			}
			cli.ShowMainMenu()
		case "0", "q", "quit", "exit":
			return
		default:
			fmt.Println("  [!] 无效选择，请重新输入 (1/2/3/0)")
		}
	}
}

// ========================================================================
// 简易测试菜单导航
// ========================================================================

// RunAutoTests 简易测试模式主循环
func (cli *CLIClient) RunAutoTests(reader *bufio.Reader) {
	// 进入时清屏并显示菜单
	if cli.settings.DisplayMode >= 2 {
		clearScreen()
	}
	cli.ShowAutoTestMenu()

	for {
		choice := readInput(reader, "请选择模块: ")

		switch {
		case choice == "0" || choice == "back":
			return
		case choice == "A" || choice == "a":
			cli.runAllTests()
		default:
			// 查找模块
			mod := cli.findModule(choice)
			if mod == nil {
				fmt.Println("  [!] 无效选择")
				continue
			}
			cli.navigateCategoryMenu(reader, mod)
			// 返回后重新显示当前层级菜单
			if cli.settings.DisplayMode >= 2 {
				clearScreen()
			}
			cli.ShowAutoTestMenu()
		}
	}
}

// navigateCategoryMenu 二级菜单：场景分类
func (cli *CLIClient) navigateCategoryMenu(reader *bufio.Reader, module *TestModule) {
	// 进入时清屏并显示菜单
	if cli.settings.DisplayMode >= 2 {
		clearScreen()
	}
	cli.ShowCategoryMenu(module)

	for {
		choice := readInput(reader, "请选择场景: ")
		categories := []string{"正常", "异常", "边界"}

		switch {
		case choice == "0" || choice == "back":
			return
		case choice == "A" || choice == "a":
			cli.RunEntries(module.Entries, module.Name)
		default:
			idx := 0
			for _, c := range choice {
				if c >= '1' && c <= '3' {
					idx = int(c - '0')
					break
				}
			}
			if idx >= 1 && idx <= 3 {
				cat := categories[idx-1]
				entries := filterByCategory(module.Entries, cat)
				if len(entries) == 0 {
					fmt.Printf("  [!] %s测试 无可用用例\n", cat)
					continue
				}
				cli.navigateTestCaseMenu(reader, entries, module.Name)
				// 返回后重新显示当前层级菜单
				if cli.settings.DisplayMode >= 2 {
					clearScreen()
				}
				cli.ShowCategoryMenu(module)
			} else {
				fmt.Println("  [!] 无效选择")
			}
		}
	}
}

// navigateTestCaseMenu 三级菜单：具体测试用例
func (cli *CLIClient) navigateTestCaseMenu(reader *bufio.Reader, entries []TestEntry, moduleName string) {
	// 进入时清屏并显示菜单
	if cli.settings.DisplayMode >= 2 {
		clearScreen()
	}
	cli.ShowTestCaseList(entries)

	for {
		choice := readInput(reader, "请选择用例: ")

		switch {
		case choice == "0" || choice == "back":
			return
		case choice == "A" || choice == "a":
			cli.RunEntries(entries, moduleName)
		default:
			idx := 0
			fmt.Sscanf(choice, "%d", &idx)
			if idx >= 1 && idx <= len(entries) {
				fmt.Println()
				cli.RunEntries([]TestEntry{entries[idx-1]}, moduleName)
				fmt.Println()
			} else {
				fmt.Println("  [!] 无效选择")
			}
		}
	}
}

// runAllTests 执行所有模块的全部测试
func (cli *CLIClient) runAllTests() {
	totalPassed := 0
	totalFailed := 0
	start := time.Now()

	for _, mod := range cli.modules {
		p, f := cli.RunEntries(mod.Entries, mod.Name)
		totalPassed += p
		totalFailed += f
	}

	duration := time.Since(start)
	fmt.Println()
	fmt.Println("============================================================")
	fmt.Printf("  全部测试完成: %d 通过, %d 失败, 总计 %d, 耗时 %v\n",
		totalPassed, totalFailed, totalPassed+totalFailed, duration.Round(time.Millisecond))
	if totalFailed == 0 {
		fmt.Println("  *** ALL TESTS PASSED ***")
	}
	fmt.Println("============================================================")

	if cli.settings.AutoSave {
		filename := fmt.Sprintf("full_test_%s.md", time.Now().Format("20060102_150405"))
		if err := cli.SaveReport(filename); err != nil {
			fmt.Printf("  [!] 保存报告失败: %v\n", err)
		} else {
			fmt.Printf("  [+] 完整报告已保存: %s/%s\n", cli.settings.OutputDir, filename)
		}
	}
}

// findModule 根据ID查找测试模块
func (cli *CLIClient) findModule(id string) *TestModule {
	for i := range cli.modules {
		if cli.modules[i].ID == id {
			return &cli.modules[i]
		}
	}
	return nil
}
