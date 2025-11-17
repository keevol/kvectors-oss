@import io.vertx.core.json.*

@param JsonObject viewModel

!{var name=viewModel.getString("collectionName");}

@template.admin.layout.layout(title= "添加新向量", subtitle = "添加新向量", json = viewModel, content = @`

    <!-- Main Content Area -->
    <main class="flex-1 overflow-y-auto p-4 lg:p-6 bg-slate-800">
        <div class="max-w-4xl mx-auto">
            <!-- Action Buttons -->
            <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6 space-y-3 sm:space-y-0">
                <div class="flex items-center">
                    <i class="fas fa-database text-green-400 text-xl mr-3"></i>
                    <div>
                        <h3 class="text-lg font-medium text-green-300">向量集合：${name}</h3>
                    </div>
                </div>
                <div class="flex flex-wrap gap-3">
                    <a href="/"
                       class="bg-gradient-to-r from-slate-600 to-slate-700 hover:from-slate-700 hover:to-slate-800 text-slate-200 px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 border border-slate-500">
                        <i class="fas fa-arrow-left mr-2"></i>Back to Dashboard
                    </a>
                </div>
            </div>

            <div class="cyber-card shadow-2xl rounded-xl overflow-hidden">
                <form class="p-6 space-y-6" hx-post="/collection/vector/add" hx-swap="none">
                    <!-- Collection Name -->
                    <div>
                        <label for="collection-name" class="block text-sm font-medium text-blue-300 mb-2">
                            <i class="fas fa-database mr-2"></i>当前向量集合
                        </label>
                        <input type="text" id="collection-name" name="collection-name" required value="${name}" readonly
                               class="tech-input block w-full rounded-lg px-4 py-3 text-sm transition-all duration-200 focus:outline-none">
<%--                        <p class="mt-2 text-sm text-slate-400">Enter a unique identifier for your data collection</p>--%>
                    </div>

                    <div>
                        <label for="vector" class="block text-sm font-medium text-blue-300 mb-2">
                            <i class="	fas fa-align-justify mr-2"></i>向量值
                        </label>
                        <input type="text" id="vector" name="vector" required
                               class="tech-input block w-full rounded-lg px-4 py-3 text-sm transition-all duration-200 focus:outline-none">
                        <p class="mt-2 text-sm text-slate-400">JSON数组格式，比如"[0.01, ...]"</p>
                    </div>

                    <div>
                        <label for="rid" class="block text-sm font-medium text-blue-300 mb-2">
                            <i class="fas fa-exchange-alt mr-2"></i>关联ID
                        </label>
                        <input type="text" id="rid" name="rid"
                               class="tech-input block w-full rounded-lg px-4 py-3 text-sm transition-all duration-200 focus:outline-none">
                        <p class="mt-2 text-sm text-slate-400">关联ID（RelationID,
                            rid)用来关联当前向量与原始数据之间的关系</p>
                    </div>

                    <!-- Collection Description -->
                    <div>
                        <label for="meta" class="block text-sm font-medium text-blue-300 mb-2">
                            <i class="fas fa-info-circle mr-2"></i>附带更多元信息（JSON格式）
                        </label>
                        <textarea id="meta" name="meta" rows="3"
                                  class="tech-input block w-full rounded-lg px-4 py-3 text-sm transition-all duration-200 focus:outline-none resize-none"></textarea>
                        <p class="mt-2 text-sm text-slate-400">为新添加的向量添加更多附加信息</p>
                    </div>

                    <!-- Submit Buttons -->
                    <div class="flex flex-col sm:flex-row justify-end space-y-3 sm:space-y-0 sm:space-x-3 pt-4">
                        <a href="#"
                           class="bg-slate-600 hover:bg-slate-500 text-slate-200 px-6 py-3 rounded-lg text-sm font-medium transition-all duration-200 border border-slate-500 text-center"
                           onclick="history.back(); return false;"
                        >
                            取消
                        </a>
                        <button type="submit"
                                class="bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white px-6 py-3 rounded-lg text-sm font-medium transition-all duration-200 glow-effect">
                            <i class="fas fa-plus mr-2"></i>马上添加！
                        </button>
                    </div>
                </form>
            </div>
        </div>
    </main>

`)


