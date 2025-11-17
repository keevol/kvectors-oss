@import io.vertx.core.json.*

@param JsonObject viewModel

@template.admin.layout.layout(title= "添加新 Vector Collection", subtitle = "添加新的 Vector Collection", json = viewModel, content = @`

    <!-- Main Content Area -->
    <main class="flex-1 overflow-y-auto p-4 lg:p-6 bg-slate-800">
        <div class="max-w-4xl mx-auto">
            <!-- Action Buttons -->
            <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6 space-y-3 sm:space-y-0">
                <div class="flex items-center">
                    <i class="fas fa-database text-green-400 text-xl mr-3"></i>
                    <div>
                        <h3 class="text-lg font-medium text-green-300">添加新向量集合</h3>
                    </div>
                </div>
                <div class="flex flex-wrap gap-3">
                    <a href="/"
                       class="bg-gradient-to-r from-slate-600 to-slate-700 hover:from-slate-700 hover:to-slate-800 text-slate-200 px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 border border-slate-500">
                        <i class="fas fa-arrow-left mr-2"></i>返回后台首页
                    </a>
                </div>
            </div>

            <div class="cyber-card shadow-2xl rounded-xl overflow-hidden">
                <form class="p-6 space-y-6" action="/collection/add" method="post">
                    <!-- Collection Name -->
                    <div>
                        <label for="collection-name" class="block text-sm font-medium text-blue-300 mb-2">
                            <i class="fas fa-database mr-2"></i>向量集合名称
                        </label>
                        <input type="text" id="collection-name" name="collection-name" required
                               class="tech-input block w-full rounded-lg px-4 py-3 text-sm transition-all duration-200 focus:outline-none">
                        <p class="mt-2 text-sm text-slate-400">Enter a unique identifier for your data collection</p>
                    </div>

                    <div>
                        <label for="index-strategy" class="block text-sm font-medium text-blue-300 mb-2">
                            <i class="fas fa-icons mr-2"></i>向量集合类型
                        </label>
                        <select id="index-strategy" name="index-strategy"
                                class="tech-input block w-full rounded-lg px-4 py-3 text-sm transition-all duration-200 focus:outline-none">
                            <option value="no-index">不创建索引类型</option>
                            <option value="ann">创建ANN索引类型</option>
                        </select>
                    </div>

                    <div>
                        <label for="compress" class="block text-sm font-medium text-blue-300 mb-2">
                            <i class="fas fa-icons mr-2"></i>存储的压缩策略
                        </label>
                        <select id="compress" name="compress"
                                class="tech-input block w-full rounded-lg px-4 py-3 text-sm transition-all duration-200 focus:outline-none">
                            <option value="no" selected>不压缩</option>
                            <option value="lz4">LZ4 压缩</option>
                            <option value="zstd">ZSTD 压缩</option>
                        </select>
                    </div>

                    <div>
                        <label for="in-memory" class="block text-sm font-medium text-blue-300 mb-2">
                            <i class="fas fa-icons mr-2"></i>向量集合的存储策略
                        </label>
                        <select id="in-memory" name="in-memory"
                                class="tech-input block w-full rounded-lg px-4 py-3 text-sm transition-all duration-200 focus:outline-none">
                            <option value="no" selected>文件系统存储</option>
                            <option value="yes">全内存存储</option>
                        </select>
                    </div>

                    <!-- Submit Buttons -->
                    <div class="flex flex-col sm:flex-row justify-end space-y-3 sm:space-y-0 sm:space-x-3 pt-4">
                        <a href="#"
                           class="bg-slate-600 hover:bg-slate-500 text-slate-200 px-6 py-3 rounded-lg text-sm font-medium transition-all duration-200 border border-slate-500 text-center"
                           onclick="history.back();return false; ">
                            取消操作
                        </a>
                        <button type="submit"
                                class="bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white px-6 py-3 rounded-lg text-sm font-medium transition-all duration-200 glow-effect">
                            <i class="fas fa-plus mr-2"></i>马上创建！
                        </button>
                    </div>
                </form>
            </div>
        </div>
    </main>

`)
